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

#include "fe_cache.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace fawn;

bool DEBUG = false;

Cache::Cache(int m) {
    old  = new CacheTable();
    cur  = new CacheTable();
    sizeOld = 0;
    sizeCur = 0;
    sizeMax = m;
    num_hits = 0;
    num_lookups = 0;
    cout << "The cache is able to cache up to "<< sizeMax << " entries" << endl;
}

bool Cache::lookup(const DBID& dkey, std::string& value) {
    bool found = false;
    CacheTable::const_accessor a;
    num_lookups ++;
    
    if (cur->find(a, dkey.actual_data_str())) {
        value = a->second;
        num_hits ++;
		//if (DEBUG)
        //	cout << "found " << dkey.actual_data_str() << " in cur:" << value << endl;
        found = true;
    }
    else if (old->find(a, dkey.actual_data_str())) {
        value = a->second;
        num_hits ++;
        //if (DEBUG)
		//	cout << "found " << dkey.actual_data_str() << " in old:" << value << endl;
        //insert(dkey, value);
        //if (DEBUG)
		//	cout << "move " << dkey.actual_data_str() << " to cur" << endl;
        found = true;
    }
    
    if (DEBUG) {
        if (found) 
            cout << "cache hit" << endl;
        else
            cout << "cache miss" << endl;
        cout << "total lookup=" << num_lookups << ",total hit=" << num_hits << endl;
    }
    return found;
}

void Cache::insert(const DBID& dkey, const std::string& value) {
    CacheTable *tmp;
    CacheTable::accessor a;
    if (sizeCur >= sizeMax/2) {
        sizeOld = sizeCur;
        old->clear();
        tmp = old;
        old = cur;
        cur = tmp;
        sizeCur = 0;
		//if (DEBUG)
        //	cout << "abandon old, point cur to old, now size =" << sizeCur << "/" << sizeOld << endl;;
    }
    cur->insert(a, dkey.actual_data_str());
    a->second = value;
    a.release();
    sizeCur ++;
	if (DEBUG)
    	cout << "insert to cur, now size =" << sizeCur << "/" << sizeOld << endl;
}
