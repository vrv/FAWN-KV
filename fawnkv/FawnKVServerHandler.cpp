/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <transport/TBufferTransports.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TSocket.h>

#include "FawnKVServerHandler.h"

#include <sys/time.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace fawn;

Cache* cache;
map<int32_t, ClientData*> cid_client_map;
map<unsigned int, ClientData*> req_client_map;
pthread_mutex_t *p_mutex;
pthread_mutex_t *p_mutex2;

double t, r;
double t_since = -1;
int64_t reqSince;
timeval tim;

void put_cb(unsigned int continuation)
{
    // continuation is the server-side continuation
    // Need to map back to client continuation

    pthread_mutex_lock(p_mutex);
    //cout << "put_cb for " << continuation << endl;
    ClientData *cd = req_client_map[continuation];
    int64_t cli_continuation = cd->getContinuation(continuation);
    cd->removeContinuation(continuation);

    // Send to client
    cd->fc->put_response(cli_continuation);
    //cout << "put_cb done for " << continuation << endl;
    pthread_mutex_unlock(p_mutex);
}

void put_w_cb(unsigned int continuation, int64_t version)
{
    cout << "WARNING: put_w is not implemented for remote frontends" << endl;
    exit(1);
}

void get_cb(const DBID& p_key, const string& p_value, unsigned int continuation, bool success)
{
    pthread_mutex_lock(p_mutex);
    //cout << "get_cb for " << continuation << endl;
    ClientData *cd = req_client_map[continuation];
    int64_t cli_continuation = cd->getContinuation(continuation);
    cd->removeContinuation(continuation);

    // Send to client
    cd->fc->get_response(p_value, cli_continuation);

    //if (cache)
    //    cache->insert(p_key, p_value);
    //cout << "get_cb done for " << continuation << endl;
    pthread_mutex_unlock(p_mutex);
}

FawnKVServerHandler::FawnKVServerHandler(FrontEnd *fe, Cache *c) {
    // Your initialization goes here
    frontend = fe;
    cache = c;
    frontend->register_put_cb((void (*)(unsigned int)) &put_cb);
    frontend->register_put_w_cb((void (*)(unsigned int, int64_t)) &put_w_cb);
    frontend->register_get_cb((void (*)(const fawn::DBID&, const std::string&, unsigned int, bool)) &get_cb);
    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&mutex2, NULL);
    p_mutex = &mutex;
    p_mutex2 = &mutex2;
    clientID = 0;
    reqNumber = 0;
	gettimeofday(&tim, NULL);
}

ClientData* FawnKVServerHandler::get_client(const int32_t cid) {
    map<int32_t,ClientData*>::iterator it = cid_client_map.find(cid);
    if (it != cid_client_map.end()) {
        return cid_client_map[cid];
    }
    return NULL;
}

int32_t FawnKVServerHandler::init(const std::string& clientIP, const int32_t port) {
    cout << "Connection from " << clientIP << ":" << port << endl;
    shared_ptr<TTransport> socket(new TSocket(clientIP.data(), port));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    FawnKVAppClient *fc = new FawnKVAppClient(protocol);

    while(1) {
        try {
            transport->open();
            break;
        } catch (TException &tx) {
            fprintf(stderr, "Transport error: %s\n", tx.what());
        }
        sleep(1);
    }

    // New client, allocate
    ClientData *cd = new ClientData(fc); // Need to destroy at some point
    int cid = clientID++;
    cid_client_map[cid] = cd;
    return cid;
}

void FawnKVServerHandler::get(const std::string& key, const int64_t continuation, const int32_t cid) {
    pthread_mutex_lock(p_mutex);
    // Generating unique server-side continuation
	//cout << reqNumber << endl;
    int64_t ss_continuation = reqNumber++;

	ClientData *cd = get_client(cid);
    DBID dkey(key);
    string value;
    if ((cache) && (cache->lookup(dkey, value))){
        cd->fc->get_response(value, continuation);
    }
    else {
        //cout << "Get from CID: " << cid << " continuation: " << continuation << " reqNumber: " << ss_continuation << endl;
        cd->addContinuation(continuation, ss_continuation);
        req_client_map[ss_continuation] = cd;
        frontend->get(key, ss_continuation);
        //cout << "Get Done for CID: " << cid << " continuation: " << continuation << " reqNumber: " << ss_continuation << endl;
    }

	if ((reqNumber % 2000) == 0) {
//    if (1) {
		gettimeofday(&tim, NULL);
		t  = tim.tv_sec + (tim.tv_usec/1000000.0);
		if ((t_since > 0) && (t-t_since > 0)) {
			double rr = (reqNumber - reqSince)/1000.00/(t-t_since);
            r = 0.8 * r + 0.2 * rr;
			cout << r << "k query/sec " ; //<<  t  - t_since << endl;
            if (cache)
                cout << "hit_ratio " << cache->hit_ratio() << " cache_size " << cache->size() << endl;
            else
                cout << "hit_ratio " << 0 << endl;
		}

		t_since = t;
		reqSince = reqNumber;
	}

    pthread_mutex_unlock(p_mutex);

}

void FawnKVServerHandler::put(const std::string& key, const std::string& value, const int64_t continuation, const int32_t cid) {
    pthread_mutex_lock(p_mutex);
    // Generating unique server-side continuation
    int64_t ss_continuation = reqNumber++;
    //cout << "Put from CID: " << cid << " continuation: " << continuation << " reqNumber: " << ss_continuation << endl;
    ClientData *cd = get_client(cid);
    cd->addContinuation(continuation, ss_continuation);
    req_client_map[ss_continuation] = cd;
    string v = value;
    frontend->put(key, value, ss_continuation);
    DBID dkey(key);
    if (cache)
        cache->insert(dkey, value);
    //cout << "Put Done for CID: " << cid << " continuation: " << continuation << " reqNumber: " << ss_continuation << endl;
    pthread_mutex_unlock(p_mutex);

}

void FawnKVServerHandler::remove(const std::string& key, const int64_t continuation, const int32_t cid) {
    // Generating unique server-side continuation
    int64_t ss_continuation = reqNumber++;
    ClientData *cd = get_client(cid);
    cd->addContinuation(continuation, ss_continuation);
    req_client_map[ss_continuation] = cd;
    pthread_mutex_lock(p_mutex);
    frontend->remove(key, ss_continuation);
    pthread_mutex_unlock(p_mutex);
}


void usage()
{
    cerr <<  "./frontend -m ManagerIP -p myPort -l listenPort -c cacheSize myIP\n\n"
         << endl;
}


int main(int argc, char **argv)
{
    int ch;
    string managerIP;
    int myPort = FE_SERVER_PORT_BASE;
    int listenPort = FE_SERVER_PORT_LISTEN;
    int cacheSize = 0;
    while ((ch = getopt(argc, argv, "m:p:l:c:")) != -1) {
        switch (ch) {
        case 'm':
            managerIP = optarg;
            break;
        case 'p':
            myPort = atoi(optarg);
            break;
        case 'l':
            listenPort = atoi(optarg);
            break;
        case 'c':
            cacheSize = atoi(optarg);
            break;
        default:
            usage();
            exit(-1);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc < 1) {
        usage();
        exit(-1);
    }

    string myIP(argv[0]);

    /* Initialize ring */
    FrontEnd *fe = new FrontEnd(managerIP, myIP, myPort);

    /* Initialize cache */
    if (cacheSize > 0)
        cache = new Cache(cacheSize);
    else
        cache = NULL;

    shared_ptr<FawnKVServerHandler> handler(new FawnKVServerHandler(fe, cache));
    shared_ptr<TProcessor> processor(new FawnKVProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(listenPort));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    cout << "Exiting front-end manager." << endl;

    return 0;
}

