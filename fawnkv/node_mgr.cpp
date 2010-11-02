/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <map>
#include <queue>
#include <iostream>
#include <string>
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include <netinet/tcp.h>

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TSocket.h>

#include "fawnnet.h"
#include "node_mgr.h"
#include "dbid.h"
#include "print.h"

using fawn::DBID;
using namespace fawn;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace boost;

bool node_mgr::b_done = false;

node_mgr::node_mgr(string managerIP, string localIP, string stat_filename,
                   bool overwriteDS)
{
    init(managerIP, localIP, stat_filename);
    overwrite = overwriteDS;
}

node_mgr::node_mgr(string managerIP, string localIP, string stat_filename,
                   const list<string>* p_vids, bool overwriteDS)
{
    init(managerIP, localIP, stat_filename);
    if (overwriteDS) {
        // create p_sids by shifting p_vid list by 1
        list<string> p_sids(*p_vids);
        p_sids.push_front(p_sids.back());
        p_sids.pop_back();
        createIntervalFiles(p_vids, &p_sids);
    } else {
        openIntervalFiles(p_vids);
    }
}

void node_mgr::init(string managerIP, string localIP, string stat_filename)
{
    time = 0;
    start_time = false;
    myIP = localIP;

    b_hb = false;
    curr_vnode = 0;

    /* Initialize mutex and condition variable objects */
    pthread_rwlock_init(&interval_lock, NULL);
    pthread_mutex_init(&socket_lock, NULL);

    num_puts = num_gets = num_put_ws = 0;
    total_num_puts = total_num_gets = total_num_put_ws = 0;
    p_start = (struct timeval*) malloc(sizeof(struct timeval));
    p_end = (struct timeval*) malloc(sizeof(struct timeval));
    gettimeofday (p_start, NULL);

    if (stat_filename.empty()) {
        stat_filename = "stat_";
    }
    stat_filename += myIP;
    cout << "stat file = " << stat_filename << endl;
    stat_file.open(stat_filename.c_str(), ios::in | ios::out | ios::trunc);
    if (!stat_file.is_open()) {
        perror("Could not open file\n");
        exit(1);
    }


    shared_ptr<TTransport> socket(new TSocket(managerIP.data(), MANAGER_PORT_BASE));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    manager_transport = transport;
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(manager_transport));
    manager = new FawnKVManagerClient(protocol);


    while (1) {
        try {
            manager_transport->open();
            break;
        } catch (TException &tx) {
            fprintf(stderr, "Transport error: %s\n", tx.what());
        }
        sleep(3);
    }
}

node_mgr::~node_mgr()
{
    try {
        manager_transport->close();
        delete manager;
    } catch (TException &tx) {
        fprintf(stderr, "Transport error: %s\n", tx.what());
    }

    /* Cleanup pthreads */
    pthread_rwlock_destroy(&interval_lock);
    pthread_mutex_destroy(&socket_lock);
    free(p_start);
    free(p_end);

    stat_file.close();

    close_files();
    for (uint i = 0; i < dbs.size(); i++) {
        pthread_rwlock_destroy(&(dbs[i]->dbLock));
        delete dbs[i]->h;
        delete dbs[i];
    }
}

FawnKVFrontendClient* node_mgr::connectTCP_FE(string IP, int port)
{
    shared_ptr<TTransport> socket(new TSocket(IP.data(), port));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    FawnKVFrontendClient* frontend = new FawnKVFrontendClient(protocol);

    try {
        transport->open();
    } catch (TException &tx) {
        fprintf(stderr, "Transport error: %s\n", tx.what());
    }

    return frontend;
}

FawnKVBackendClient* node_mgr::connectTCP(string IP, int port)
{
    shared_ptr<TTransport> socket(new TSocket(IP.data(), port));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    FawnKVBackendClient* backend = new FawnKVBackendClient(protocol);

    try {
        transport->open();
    } catch (TException &tx) {
        fprintf(stderr, "Transport error: %s\n", tx.what());
    }

    return backend;
}

/**
 * vid = id for vnode,
 * nid = neighbor (succ or pred)
 * nip = neighbor IP (succ or pred)
 * nip now includes :port
 */
FawnKVFrontendClient* node_mgr::connect_to_frontend(const string& nip)
{
    FawnKVFrontendClient* frontend = NULL;
    string ip = getIP(nip);
    int32_t port = getPort(nip);
    map<string,FawnKVFrontendClient*>::iterator it = ip_fe_map.find(nip);
    if (it == ip_fe_map.end()) {
        frontend = connectTCP_FE(ip, port);
        // assert(sock != -1);
        ip_fe_map[nip] = frontend;

        // Create lock for socket for persistent client connections
        pthread_mutex_t *fe_lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(fe_lock, NULL);
        ip_socketlock_map[nip] = fe_lock;
    } else {
        frontend = ip_fe_map[nip];
        DPRINTF(DEBUG_FLOW, "Connection to %s already established\n", nip.c_str());
    }

    return frontend;
}

/**
 * vid = id for vnode,
 * nid = neighbor (succ or pred)
 * nip = neighbor IP (succ or pred)
 * nip now includes :port
 */
FawnKVBackendClient* node_mgr::connect_to_backend(const string& vid, const string& nid, const string& nip, bool bpred)
{
    FawnKVBackendClient* backend = NULL;
    string ip = getIP(nip);
    int32_t port = getPort(nip);
    map<string,FawnKVBackendClient*>::iterator it = ip_client_map.find(getID(ip, port));
    if (it == ip_client_map.end()) {
        backend = connectTCP(ip, port);
        // assert(sock != -1);
        ip_client_map[nip] = backend;

        // Create lock for socket for persistent client connections
        pthread_mutex_t *client_lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(client_lock, NULL);
        ip_socketlock_map[nip] = client_lock;

        //cout << "my_id " << bytes_to_hex(vid) << " connected to vid " << bytes_to_hex(nid) << " @IP " << nip << endl;
    } else {
        // This might occur if this IP is a neighbor for another vid, so don't close it!
        //cout << "Connection already established " << endl;
        backend = ip_client_map[nip];
        DPRINTF(DEBUG_FLOW, "Connection to %s already established\n", nip.c_str());
    }

    if (vid != "") {
        pair <string, string> p (nid, nip);
        if (bpred) {
            vid_pid_predIP_map[vid] = p;
            cout << "pred mapping " << bytes_to_hex(vid) << " --> <" << bytes_to_hex(nid) << " , " << nip << "> created" << endl;
        } else {
            vid_sid_succIP_map[vid] = p;
            cout << "succ mapping " << bytes_to_hex(vid) << " --> <" << bytes_to_hex(nid) << " , " << nip << "> created" << endl;
        }
    }

    // TODO: if not done when CMM is received, at some point go through
    // all the neighbours (preds, succs) and close connections to those who are
    // not neighbours (those not in preds & succs but still in ip_client_map)

    //cleanup_sockets();

    return backend;
}

// Resets whatever connection is associated with this ip port.
void node_mgr::init(const string& ip, int32_t port)
{
    string nip = getID(ip, port);
    map<string,FawnKVFrontendClient*>::iterator it = ip_fe_map.find(nip);
    if (it != ip_fe_map.end()) {
        delete (*it).second;
        ip_fe_map.erase(it);
    }

}

void node_mgr::cleanup_sockets()
{
    // iterate through ip_client_map.
    // if any ip exists that is not a pair in vid_sid_succIP_map or vid_pid_predIP_map, close it.

    map<string,FawnKVBackendClient*>::iterator it;
    for (it = ip_client_map.begin(); it != ip_client_map.end(); it++) {
        string ip = (*it).first;
        FawnKVBackendClient* backend = (*it).second;

        bool found = false;
        map<string, pair<string,string> >::iterator it2;
        for (it2 = vid_sid_succIP_map.begin();
                (it2 != vid_sid_succIP_map.end()) && (found != true);
                it2++) {
            pair <string, string> p = (*it2).second;
            string succ = p.second;
            if (ip == succ) {
                found = true;
            }
        }

        for (it2 = vid_pid_predIP_map.begin();
                (it2 != vid_pid_predIP_map.end()) && (found != true);
                it2++) {
            pair <string, string> p = (*it2).second;
            string succ = p.second;
            if (ip == succ) {
                found = true;
                break;
            }
        }


        // This IP was not found in the vid->succ map, so it was not a successor for ANY vid.
        if (!found) {
            DPRINTF(DEBUG_FLOW, "Closing unused socket.\n");
            delete backend; // Does this close the transport too?
            pthread_mutex_t* client_lock = ip_socketlock_map[ip];
            pthread_mutex_destroy(client_lock);
            free(client_lock);
            ip_socketlock_map.erase(ip);
        }
    }

}

/* the next 2 functions take in a vid that the current node is assigned and
   returns the socket for either the successor or the predecessor for that
   particular vid_pid_predIP_map */

// takes vid in raw format (data from ID)
FawnKVBackendClient* node_mgr::get_pred_client(const string& vid) {
    FawnKVBackendClient* backend = NULL;
    map< string, pair<string, string> >::iterator it = vid_pid_predIP_map.find(vid);
    if (it != vid_pid_predIP_map.end()) {
        pair<string, string> p = (*it).second;
        map<string,FawnKVBackendClient*>::iterator it2 = ip_client_map.find(p.second);
        if (it2 != ip_client_map.end()) {
            backend = (*it2).second;
        }
    }
    return backend;
}


// takes vid in raw format (data from ID)
FawnKVBackendClient* node_mgr::get_successor_client(const string& vid) {
    FawnKVBackendClient* backend = NULL;
    map< string, pair<string, string> >::iterator it = vid_sid_succIP_map.find(vid);
    if (it != vid_sid_succIP_map.end()) {
        pair <string, string> p = (*it).second;
        map<string,FawnKVBackendClient*>::iterator it2 = ip_client_map.find(p.second);
        if (it2 != ip_client_map.end()) {
            backend = (*it2).second;
        }
    }
    return backend;
}

void node_mgr::close_files()
{
    printf("Trying to close files cleanly\n");
    bool clean = true;
    for (uint i = 0; i < dbs.size(); i++) {
        if (!dbs[i]->h->WriteHashtableToFile()) {
            clean = false;
            printf("Unable to write hashtable for %s\n", dbs[i]->name.c_str());
        }
    }
    if (clean) {
        printf("All files closed properly\n");
    }
}


int node_mgr::sendStaticJoinRequest()
{
    cout << "Sending StaticJoin Request from IP: " << myIP << ":" << myPort << endl;
    pthread_mutex_lock(&socket_lock);
    manager->static_join(myIP, myPort);
    pthread_mutex_unlock(&socket_lock);

    return 0;
}

int node_mgr::sendReJoinRequest()
{
    /* Send re-join request on reliable channel */
    cout << "Sending ReJoin Request from IP: " << myIP << endl;

    vector<string> vids;
    vector<string> sids;

    for (uint i = 0; i < dbs.size(); i++) {

        const DBID* d_sid = dbs[i]->h->getStartID();
        const DBID* d_eid = dbs[i]->h->getEndID();
#ifdef DEBUG
        cout << "dbs[i]->beginId --- " << bytes_to_hex(d_sid->actual_data_str()) << endl;
        cout << "dbs[i]->endId --- " << bytes_to_hex(d_eid->actual_data_str()) << endl;
#endif
        string sid(d_sid->const_data(), DBID_LENGTH);
        string vid(d_eid->const_data(), DBID_LENGTH);

        vids.push_back(vid);
        sids.push_back(sid);

        delete d_sid;
        delete d_eid;
    }

    pthread_mutex_lock(&socket_lock);
    manager->rejoin(vids, sids, myIP, myPort);
    pthread_mutex_unlock(&socket_lock);
    return 0;
}

int node_mgr::sendJoinRequest()
{
    /* Send join request on reliable channel */
    cout << "Sending Join Request from IP: " << myIP << ":" << myPort << endl;
    pthread_mutex_lock(&socket_lock);
    manager->join(myIP, myPort, false);
    pthread_mutex_unlock(&socket_lock);
    return 0;
}

void node_mgr::log_stats()
{
    if (!start_time && num_gets > 0)
        start_time = true;

    gettimeofday (p_end, NULL);
    long interval_in_us = (p_end->tv_sec - p_start->tv_sec)*1000000 + p_end->tv_usec - p_start->tv_usec;
    stat_file << num_gets << "\t" << num_puts << "\t" << num_put_ws << "\t" << ( ((float)(num_gets + num_puts + num_put_ws)*1000000)/interval_in_us ) << "\t" << interval_in_us << "\t" << time << "\t" << num_network << endl;

    if (start_time)
        time++;
    num_gets = num_puts = num_put_ws = 0;
    num_network = 0;
    gettimeofday (p_start, NULL);
}


void node_mgr::print_all_db_ranges() {
    cout << "printing all valid DB ranges" << endl;
    cout << "#DB ranges to check = " << dbs.size() << endl;
    for (uint i = 0; i < dbs.size(); i++) {

        const DBID* d_sid = dbs[i]->h->getStartID();
        const DBID* d_eid = dbs[i]->h->getEndID();

        if (dbs[i]->valid)
          cout << "[" << bytes_to_hex(d_sid->actual_data_str()) << ", " << bytes_to_hex(d_eid->actual_data_str()) << "]" << endl;

        delete d_sid;
        delete d_eid;
    }
}

void node_mgr::sighandler(int sig) {
    cout<< "Signal " << sig << " caught..." << endl;

    b_done = true;
}


////////////////////////////////////////////////////////////////////////////////

void usage()
{
    cerr <<   "   -h         help (this text) \n"
         <<   "   -m         manager's IP addr \n"
         <<   "   -i         self IP addr \n"
         <<   "   -p         self port \n"
         <<   "   -v [#]     # of virtual nodes this node represents and also indicates # of db files to follow separated by spaces \n"
         <<   "   -b path    base path/name for db file, e.g.: /localfs/fawnds_ \n"
         <<   "   -t [#]     # of threads to spawn for concurrent I/O operations \n"
         <<   "   -j         static join without knowing your IDs.\n"
         <<   "   -o         overwrite existing DS if it already exists.\n"
         <<   "   -s [name]  stat file name \n";

    cerr <<   "Example: ./backend -m 128.2.223.35 -i 128.2.223.35 -b \"/localfs/fawndb_ /localfs2/fawndb_\" -o" << endl;
}


int main(int argc, char **argv)
{
    extern char *optarg;
    extern int optind;

    list<string> vids;

    string managerIP;
    string myIP = getMyIP();
    int port = BACKEND_SERVER_PORT_BASE;
    string stat_filename;
    // default
    string filebase("/localfs/fawnds_");

    int numConsumerThreads = 1;
    bool unknown_ids = false;
    bool overwriteExistingDS = false;

    int n_needed = 0;
    int ch;
    bool special_init = false;
    bool static_init = false;
    bool sendJoinMsg = true;

    while ((ch = getopt(argc, argv, "hm:i:p:v:s:b:t:n:jro")) != -1) {
        switch (ch) {
        case 'm':
            managerIP = optarg;
            n_needed++;
            break;
        case 'i':
            myIP = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 't':
            numConsumerThreads = atoi(optarg);
            break;
        case 'v':
        {
            vector<string> tokens;
            tokenize(optarg, tokens, " ");

            unsigned int num_vnodes = atoi(tokens.front().c_str());
            if (num_vnodes != (tokens.size() - 1)) {
                usage();
                exit(1);
            } else {
                vector<string>::iterator filename_iterator = tokens.begin();
                filename_iterator++;
                for ( ; filename_iterator != tokens.end(); filename_iterator++ ) {
                    vector<string> id_tokens;
                    tokenize(*filename_iterator, id_tokens, "_");
                    cout << "Virtual Node ID = " << id_tokens.at(1);
                    vids.push_back(id_tokens.at(1));

                    cout << "\t DB Filename = " << *filename_iterator << endl;
                }
                cout << "----------" << endl;
            }
            special_init = true;
        }
        break;
        case 'j':
            unknown_ids = true;
            special_init = false;
            static_init = true;
            break;
        case 'r':
            unknown_ids = false;
            special_init = true;
            static_init = false;
            break;
        case 'n':
        {
            // For single node operation with multiple fawnds files
            // Locally generate ids equally spaced around 32-bit space
            // TODO: generalize beyond 32-bit
            sendJoinMsg = false;
            special_init = true;
            int numFiles = atoi(optarg);
            uint32_t base = (((numFiles^4294967295) + 1)/numFiles) + 1;
            for (int i = 0; i < numFiles; i++) {
                uint32_t ring_id = base * i;
                DBID id((char *)&ring_id, sizeof(u_int32_t));
                string s(id.data(), id.size());
                vids.push_back(s);
            }
        }
        break;
        case 's':
            stat_filename = optarg;
            break;
        case 'b':
            filebase = optarg;
            break;
        case 'o':
            overwriteExistingDS = true;
            break;
        case 'h':
            usage();
            exit(0);
        default:
            usage();
            exit(-1);
        }
    }
    argc -= optind;
    argv += optind;

    if ((argc >= 1) || (n_needed < 1)) {
        usage();
        exit(-1);
    }

    node_mgr* nm;
    if (special_init && vids.size() != 0) {
        nm = new node_mgr(managerIP, myIP, stat_filename, &vids,
                          overwriteExistingDS);
    } else {
        nm = new node_mgr(managerIP, myIP, stat_filename,
                          overwriteExistingDS);
    }

    nm->unknown_ids = unknown_ids;
    nm->numThreads = numConsumerThreads;
    nm->myPort = port;
    nm->filebase = filebase;

    pthread_t localServerThreadId_;
    int code = pthread_create(&localServerThreadId_, NULL,
                              localServerThreadLoop, nm);

    pthread_t localDeadThreadId_;
    code = pthread_create(&localDeadThreadId_, NULL,
                          localDeadLoop, nm);

    pthread_t statsThreadId_;
    code = pthread_create(&statsThreadId_, NULL,
                          statsThreadLoop, nm);


    pthread_t* nmgrGetThreadIds_ = (pthread_t*)malloc(numConsumerThreads * sizeof(pthread_t));

    if (nmgrGetThreadIds_ == NULL) {
        cerr << "Could not allocate consumer thread array" << endl;
        exit(-1);
    }

    for (int i = 0; i < numConsumerThreads; i++) {
        pthread_create(&nmgrGetThreadIds_[i], NULL,
                       localGetThreadLoop, (void*)nm);
    }

    pthread_t localGetResponseThreadId_;
    code = pthread_create(&localGetResponseThreadId_, NULL,
                          localGetResponseThreadLoop, nm);

    // wait for the local server socket to listen
    sleep(2);

    if (sendJoinMsg) {
        if (static_init) {
            nm->sendStaticJoinRequest();
        } else if (special_init) {
            nm->sendReJoinRequest();
        } else {
            nm->sendJoinRequest();
        }
    }

    pthread_t heartbeatThreadId_;
    code = pthread_create(&heartbeatThreadId_, NULL,
                          heartbeatThreadLoop, nm);


    //    signal(SIGTERM, &node_mgr::sighandler);
    //    signal(SIGINT, &node_mgr::sighandler);

    pthread_join(localServerThreadId_, NULL);

    free(nmgrGetThreadIds_);
    cout << "bye!" << endl;

    delete nm;

    exit(0);
}

void *localServerThreadLoop(void *p)
{
    node_mgr *nm = (node_mgr*) p;

    shared_ptr<node_mgr> handler(nm);
    shared_ptr<TProcessor> processor(new FawnKVBackendProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(nm->myPort));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    cout << "Starting server on " << nm->myPort << endl;

    nm->server = new TThreadedServer(processor, serverTransport, transportFactory, protocolFactory);
    nm->server->serve();

    return NULL;
}

void *localGetThreadLoop(void *p) {
    node_mgr* nm = (node_mgr*) p;

    while (!node_mgr::b_done) {
        nm->getConsumer();
    }

    return NULL;
}

void *localGetResponseThreadLoop(void *p) {
    node_mgr* nm = (node_mgr*) p;

    while (!node_mgr::b_done) {
        nm->getResponseConsumer();
    }

    return NULL;
}

void *statsThreadLoop(void *p) {
    node_mgr* nm = (node_mgr*) p;

    while (!node_mgr::b_done) {
        nm->log_stats();
        sleep(1);
    }

    return NULL;
}

void *localDeadLoop(void *p)
{
    node_mgr *nm = (node_mgr*) p;

    while (!nm->b_done) {
        sleep(1);
    }
    nm->server->stop();

    return NULL;
}

void *splitThread(void* p) {
    splitMsg* sm = (splitMsg*)p;
    node_mgr *nm = (node_mgr*)sm->p_node_mgr;
    string* key = sm->key;

    nm->handle_split(*key);

    delete sm->key;
    free(sm);
    return NULL;
}

void *rewriteThread(void* p) {
    interval_db *idb = (interval_db*)p;
    FawnDS<FawnDS_Flash>* h = idb->h;
    FawnDS<FawnDS_Flash>* h_new = h->Rewrite(&(idb->dbLock));
    assert(h_new != NULL);
    idb->h = h_new; // pointer swap
    pthread_rwlock_unlock(&(idb->dbLock));
    assert(h_new->RenameAndClean(h) == true);
    cout << "Done Rewriting!" << endl;
    return NULL;
}

void *precopyThread(void* p) {
    precopyMsg* pm = (precopyMsg*)p;
    node_mgr *nm = (node_mgr*)pm->p_node_mgr;
    string* startKey = pm->startKey;
    string* endKey = pm->endKey;
    string* vid = pm->vid;
    string* ip = pm->ip;
    int32_t port = pm->port;

    nm->precopy_request_thread(*startKey, *endKey, *vid, *ip, port);

    delete pm->startKey;
    delete pm->endKey;
    delete pm->vid;
    delete pm->ip;
    free(pm);
    return NULL;
}

void *heartbeatThreadLoop(void *p) {
    node_mgr* nm = (node_mgr*) p;
    int i = 0;
    while (!node_mgr::b_done) {
        if ((nm != NULL)) {
            if ( i %10 == 0)
                printf("Beating my heart: %s\n", nm->myIP.c_str());
            pthread_mutex_lock(&nm->socket_lock);
            try {
                nm->manager->heartbeat(getID(nm->myIP, nm->myPort));
            } catch (TException &tx) {
                fprintf(stderr, "Transport error: %s\n", tx.what());
                fprintf(stderr, "Cannot contact manager; exiting...\n");
                node_mgr::b_done = true;
            }
            pthread_mutex_unlock(&nm->socket_lock);
        }
        sleep(AGING_INTERVAL_IN_SEC);
        i++;
    }

    return NULL;
}

