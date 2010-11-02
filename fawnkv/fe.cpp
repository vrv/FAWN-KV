/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fe.h"

FrontEnd::FrontEnd(string managerIP, string myIP, int myPort) : myIP(myIP), myPort(myPort)
{
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

    ring = new Ring();
    UpdateRingState();

    pthread_t localServerThreadId_;
    int code = pthread_create(&localServerThreadId_, NULL,
                              localServerThreadLoop, this);
}

FrontEnd::~FrontEnd()
{
    try {
        manager_transport->close();
        map<string,FawnKVBackendClient*>::iterator it;
        for (it = ip_client_map.begin(); it != ip_client_map.end(); it++) {
            FawnKVBackendClient* backend = (*it).second;
            delete backend;
        }
    } catch (TException &tx) {
        fprintf(stderr, "Transport error: %s\n", tx.what());
    }
    delete manager;
    manager = NULL;
}

void FrontEnd::UpdateRingState()
{
    // Get Ring State and connect to backends
    ringState rs;
    manager->get_ring_state(rs);
    for (uint i = 0; i < rs.nodes.size(); i++) {
        NodeData n = rs.nodes[i];
        if (getID(myIP, myPort) != getID(n.ip, n.port)) {
            assert(connect_to_backend(getID(n.ip, n.port)) != NULL);
        }
    }
    ring->UpdateState(rs);
}

int FrontEnd::put(const std::string& key, const std::string& value, const int64_t continuation)
{
    NodeHandle *h = ring->getReplicaGroup(key);
    pair<Node*, int> p = h->retrievePutTarget();
    Node* n = p.first;
    int i = p.second;
    FawnKVBackendClient* backend = ip_client_map[getID(n->IP, n->port)];
    uint32_t hops = h->replicaGroup.size() + 1 - i;
    backend->put(key, value, hops,
                 hops, continuation, 0,
                 false, false, getID(myIP, myPort));
	delete h;
    return 0;
}

int FrontEnd::put_w(const std::string& key, const std::string& value, const int64_t continuation)
{
    NodeHandle *h = ring->getReplicaGroup(key);
    pair<Node*, int> p = h->retrievePutTarget();
    Node* n = p.first;
    int i = p.second;
    FawnKVBackendClient* backend = ip_client_map[getID(n->IP, n->port)];
    uint32_t hops = h->replicaGroup.size() + 1 - i;
    backend->put_w(key, value, hops,
                 hops, continuation, 0,
                 false, false, getID(myIP, myPort));
    return 0;
}

int FrontEnd::remove(const std::string& key, const int64_t continuation)
{
    NodeHandle *h = ring->getReplicaGroup(key);
    pair<Node*, int> p = h->retrievePutTarget();
    Node* n = p.first;
    int i = p.second;
    FawnKVBackendClient* backend = ip_client_map[getID(n->IP, n->port)];
    uint32_t hops = h->replicaGroup.size() + 1 - i;
    string value;
    backend->put(key, value, hops,
                 hops, continuation, 0,
                 false, true, getID(myIP, myPort));
	delete h;
    return 0;
}

int FrontEnd::get(const std::string& key, const int64_t continuation)
{
    NodeHandle *h = ring->getReplicaGroup(key);
    DBID* vidb = ring->getSuccessorID(key);
    string vid(vidb->data(), vidb->size());
    string rvid = ring->getRangeKey(key);
    bool interim_tail = false;
    if(ring->vid_shorter_map.find(rvid) != ring->vid_shorter_map.end()) {
        interim_tail = ring->vid_temptail_map[vid] || ring->vid_shorter_map[rvid];
    } else {
        interim_tail = ring->vid_temptail_map[vid];
    }
    Node* n = h->retrieveGetTarget(interim_tail);
    string nodeID = getID(n->IP, n->port);
    FawnKVBackendClient* backend = ip_client_map[nodeID];
    backend->get(key, continuation, getID(myIP, myPort));
	delete h;
    return 0;
}


void FrontEnd::register_put_cb(void (*cb)(unsigned int)) {
    put_cb = cb;
}

void FrontEnd::register_put_w_cb(void (*cb)(unsigned int, int64_t)) {
    put_w_cb = cb;
}

void FrontEnd::register_get_cb(void (*cb)(const DBID&, const string&, unsigned int, bool)) {
    get_cb = cb;
}

void FrontEnd::put_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip) {
    // Your implementation goes here
    if (NULL != put_cb) {
        put_cb(continuation);
    }
}

void FrontEnd::put_w_response(const std::string& key, const int64_t continuation, const bool success, const int64_t version, const std::string& ip) {
    // Your implementation goes here
    if (NULL != put_cb) {
        put_w_cb(continuation, version);
    }
}

void FrontEnd::get_response(const std::string& key, const std::string& value, const int64_t continuation, const int16_t status, const std::string& ip) {
    if (status == returnStatus::STALE_RING) {
        // Update ring and try again
        UpdateRingState();
        get(key, continuation);
        return;
    }

    if (NULL != get_cb) {
        DBID dkey(key);
        bool success = (status == returnStatus::SUCCESS);
        get_cb(dkey, value, continuation, success);
    }
}

void FrontEnd::remove_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip) {
    // Your implementation goes here
    printf("remove_response\n");
    if (NULL != put_cb) {
        put_cb(continuation);
    }
}


FawnKVBackendClient* FrontEnd::connect_to_backend(const string& nip)
{
    cout << "connecting to " << nip << endl;
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

    return backend;
}

FawnKVBackendClient* FrontEnd::connectTCP(string IP, int port)
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

    backend->init(myIP, myPort);
    return backend;


}

void *localServerThreadLoop(void *p)
{
    FrontEnd *fe = (FrontEnd*) p;

    shared_ptr<FrontEnd> handler(fe);
    shared_ptr<TProcessor> processor(new FawnKVFrontendProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(fe->myPort));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return NULL;
}
