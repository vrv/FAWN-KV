#ifndef _RING_H_
#define _RING_H_

#include <list>
#include <queue>
#include <string>
#include <pthread.h>
#include <tbb/concurrent_queue.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <map>

#include "dbid.h"
#include "virtualnode.h"
#include "nodehandle.h"
#include "node.h"
#include "debug.h"
#include "FawnKVFrontend.h"
#include "FawnKVBackend.h"
#include "print.h"

#include "hashutil.h"

using fawn::DBID;
using namespace std;
using namespace tbb;


class Ring {
 public:
    Ring();
    ~Ring();

    void UpdateState(ringState rs);

    string getRangeKey(string key);
    NodeHandle* getReplicaGroup(string key);
    DBID* getSuccessorID(string key);

    /* Util */
    void printNodes();

    map<string, Node*> ip_node_map;
    map <string, bool> vid_temptail_map;
    map <string, bool> vid_shorter_map;

 private:
    Node* createNode(string ip, int port);
    /* A ring is a list of virtual nodes */
    std::list<VirtualNode*> nodeList;
    uint32_t replica_group_size;


};

#endif
