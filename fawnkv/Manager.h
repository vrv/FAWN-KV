/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _MANAGER_H_
#define _MANAGER_H_

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
#include "FawnKVManager.h"
#include "FawnKVBackend.h"
#include "print.h"

#include "hashutil.h"

using fawn::DBID;
using namespace std;
using namespace tbb;


#define MANAGER_PORT_BASE 4000

void *localServerThreadLoop(void *p);
void *ringStart(void *p);

////////////////////////////////////////////////////////////////////////////////


//const int NUM_VIRTUAL_IDS = 8;
const int NUM_VIRTUAL_IDS = 1;
const int NUM_PHY_NODES = 15;

const int AGING_INTERVAL_IN_SEC = 5; // Beat interval
const int NUM_BEATS_BEFORE_FAILURE = 3;

class Manager : virtual public FawnKVManagerIf {
 public:
    Manager(string localIP, int num_replicas = 1);
    void init(string localIP, int num_replicas);
    ~Manager();

    void sendNeighborUpdates();

    void static_join(const std::string& ip, const int32_t port);
    void rejoin(const std::vector<std::string> & vnodeids, const std::vector<std::string> & startids, const std::string& ip, const int32_t port);
    void join(const std::string& ip, const int32_t port, bool merge=false);
    void vnode_pre_join(const std::string& vid, const std::string& ip, const int32_t port);
    void vnode_join(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge);
    void vnode_join_extend_chain(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge);
    void flush_split(const std::string& start_id, const std::string& end_id, const std::string& vid, const std::string& joiner_ip, const std::vector<std::string> & forwarding_ips, const std::vector<std::string> & neighbor_ips, const std::vector<std::string> & neighbor_vids, const int32_t hops, const std::string& ip);
    void flush_nosplit(const std::string& vid);
    void heartbeat(const std::string& ip);
    void chain_repair_done(const std::string& key,const std::string& endkey);

    void verifyThrift(VirtualNode* failed);

    void get_ring_state(ringState& _return);

    /* Management Procedures */
    void sendInitJoinResponses();
    VirtualNode** joinRequest(string ip);
    void joinIntermediate(string ip);

    NodeHandle* getReplicaGroup(DBID* k);

    /* Util */
    void printNodes();

    void ageNodes();

    map<string, Node*> ip_node_map;

 private:

    string myIP;
    map <string, bool> vid_temptail_map;
    map <string, bool> vid_shorter_map;

    pthread_mutex_t phy_nodelist_mutex;
    pthread_mutex_t virtual_nodelist_mutex;

    pthread_mutex_t count_mutex;
    pthread_cond_t count_threshold_cv;

    int numJoinedPhyNodes;
    int numNodesExpected; // Used to evenly distribute ring space for easier testing.

    Node* createNode(string ip, int port);

    void failNode(Node*);

    /* A ring is a list of virtual nodes */
    std::list<VirtualNode*> nodeList;

    /* Joined Physical Nodes */
    std::list<Node*> phyNodeList;

    /* Data is replicated across a replica group of this size */
    unsigned int replica_group_size;

    void sendJoinRsp(Node* n);
    void sendReJoinRsp(Node* n);
    // reJoin directly adds node with id = vid to the ring
    void staticJoin(const DBID id, Node* n, bool lock = true);
    // Add NUM_VIRTUAL_IDS virtual nodes to ring
    void joinRing(string ip, int32_t port, Node* n);

    /* Management Helpers */
    /* Add node to ring in ascending order*/
    void addNodetoRing(VirtualNode* n, bool lock = true);
    void printRing();
    void removeNodefromRing(Node* n, bool lock = true);
    Node** getCCWDescendants(VirtualNode* vn);

    VirtualNode* getPredecessor(DBID* vn, bool b_lock);
    DBID* getPredecessorID(DBID* vn, bool b_lock);
    VirtualNode* getSuccessor(DBID* vn, bool b_lock);
    DBID* getSuccessorID(DBID* vn, bool b_lock);
    vector<string>* getNbrhood(DBID* vn);

    VirtualNode* getDistinctPredecessor(DBID* vn);
    VirtualNode* getDistinctSuccessor(DBID* vn);
    void print_membership_info();

    DBID* getRangeKey(DBID* key);
    void removeVirtualNodefromRing(DBID* id, bool lock=true);
    void removePhysicalNode(Node *n);
};

#endif
