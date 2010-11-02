/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _TFAWNKV_H_
#define _TFAWNKV_H_

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

#include "FawnKV.h"
#include "FawnKVApp.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace tbb;
using namespace std;

using boost::shared_ptr;

using namespace fawn;

class FawnKVClt {
 private:
    TSimpleServer *server;
    FawnKVClient *c;
    int32_t cid;
    int64_t continuation;
    string myIP;
    uint16_t myPort;

 public:
    pthread_cond_t client_cv;
    pthread_mutex_t client_mutex;
    bool has_data;
    string data;

    FawnKVClt(const std::string& frontendIP, const int32_t port, const std::string& clientIP = "", const int32_t clientPort = 0);
    ~FawnKVClt();

    string get(const std::string& key);
    void put(const std::string& key, const std::string& value);
    void remove(const std::string& key);
};

class FawnKVClientHandler : virtual public FawnKVAppIf {
 private:
    FawnKVClt *fc;
 public:
    FawnKVClientHandler(FawnKVClt *f);

    void get_response(const std::string& value, const int64_t continuation);
    void put_response(const int64_t continuation);
    void remove_response(const int64_t continuation);
};

#endif
