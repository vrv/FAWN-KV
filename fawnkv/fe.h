/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FE_H_
#define _FE_H_

#include <iostream>
#include <sys/time.h>
#include "dbid.h"
#include "ring.h"
#include "FawnKVFrontend.h"
#include "FawnKVManager.h"

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace std;
using namespace fawn;

using fawn::DBID;

#define MANAGER_PORT_BASE 4000

void *localServerThreadLoop(void *p);

class FrontEnd : virtual public FawnKVFrontendIf
{
 private:
    string myIP;
    Ring *ring;

    void (*put_cb)(unsigned int); // continuation
    void (*put_w_cb)(unsigned int, int64_t); // continuation, version
    void (*get_cb)(const DBID&, const string&, unsigned int, bool); // key, val, continuation, success

    shared_ptr<TTransport> manager_transport;
    map<string, FawnKVBackendClient*> ip_client_map;
    map<string, pthread_mutex_t*> ip_socketlock_map;

    void UpdateRingState();

    // Copied from node_mgr; should unify in "connection manager"
    FawnKVBackendClient* connect_to_backend(const string& nip);
    FawnKVBackendClient* connectTCP(string IP, int port);


 public:
    FawnKVManagerClient *manager;

    explicit FrontEnd(string managerIP, string myIP, int myPort);
    virtual ~FrontEnd();

    int myPort;


    /* Client Callback registration */
    void register_put_cb(void (*cb)(unsigned int));
    void register_put_w_cb(void (*cb)(unsigned int, int64_t));
    void register_get_cb(void (*cb)(const DBID&, const string&, unsigned int, bool));

    int put(const std::string& key, const std::string& value, const int64_t continuation);
    int put_w(const std::string& key, const std::string& value, const int64_t continuation);
    int remove(const std::string& key, const int64_t continuation);
    int get(const std::string& key, const int64_t continuation);

    void put_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip);
    void put_w_response(const std::string& key, const int64_t continuation, const bool success, const int64_t version, const std::string& ip);
    void get_response(const std::string& key, const std::string& value, const int64_t continuation, const int16_t status, const std::string& ip);
    void remove_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip);

};

#endif
