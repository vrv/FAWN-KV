/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FECACHE_H_
#define _FECACHE_H_

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
#include <tbb/concurrent_hash_map.h>
#include <time.h>

#include "ring.h"
#include "FawnKV.h"
#include "FawnKVApp.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace tbb;

using boost::shared_ptr;

using namespace fawn;

typedef concurrent_hash_map<string,string> CacheTable;

class Cache {
private:
    CacheTable *old;
    CacheTable *cur;
    atomic<int32_t> sizeCur;
    atomic<int32_t> sizeOld;
    atomic<int32_t> num_hits;
    atomic<int32_t> num_lookups;

public:
    Cache(int m);
    bool lookup(const DBID& dkey, std::string& value);
    void insert(const DBID& dkey, const std::string& value);
    int sizeMax;
    int  size() {
        return sizeOld + sizeCur;
    }
    double hit_ratio() {
        return 1.0 * num_hits / num_lookups;
    }
    double load_factor() {
        return 1.0 * (sizeOld + sizeCur) / sizeMax;
    }
};

#endif
