/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNKVSERVERHANDLER_H_
#define _FAWNKVSERVERHANDLER_H_

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
#include <tbb/atomic.h>

#include "fe.h"
#include "fe_cache.h"
#include "FawnKV.h"
#include "FawnKVApp.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace tbb;

using boost::shared_ptr;

using namespace fawn;

#define FE_SERVER_PORT_LISTEN 4001
#define FE_SERVER_PORT_BASE 7000

class ClientData {
private:
    map<int64_t, int64_t> req_cont_map;
public:
    FawnKVAppClient *fc;

    ClientData(FawnKVAppClient* client) {
        fc = client;
    }

    void addContinuation(int64_t continuation, int64_t req) {
        req_cont_map[req] = continuation;
    }

    int64_t getContinuation(int64_t req) {
        return req_cont_map[req];
    }

    void removeContinuation(int64_t req) {
        req_cont_map.erase(req);
    }
};

class FawnKVServerHandler : virtual public FawnKVIf {
private:
    atomic<int32_t> clientID;
    atomic<int64_t> reqNumber;

    FrontEnd* frontend;

    ClientData* get_client(const int32_t cid);

public:
    FawnKVServerHandler(FrontEnd *fe, Cache* c);
    pthread_mutex_t mutex;
    pthread_mutex_t mutex2;

    int32_t init(const std::string& clientIP, const int32_t port);
    void get(const std::string& key, const int64_t continuation, const int32_t cid);
    void put(const std::string& key, const std::string& value, const int64_t continuation, const int32_t cid);
    void remove(const std::string& key, const int64_t continuation, const int32_t cid);
};

#endif
