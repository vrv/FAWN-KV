/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _NODE_MANAGER_H_
#define _NODE_MANAGER_H_

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>
#include <map>
#include <queue>
#include <pthread.h>
#include <string>
#include <sys/time.h>
#include <fstream>
#include <signal.h>
#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <transport/TTransportUtils.h>
#include <server/TThreadedServer.h>

#include "dbid.h"
#include "fawnds.h"
#include "fawnds_flash.h"
#include "hash_functions.h"
#include "debug.h"
#include "timing.h"
#include "FawnKVBackend.h"
#include "FawnKVFrontend.h"
#include "FawnKVManager.h"
#include "ring.h"

using namespace fawn;
using namespace tbb;
using fawn::DBID;
using namespace std;
using namespace boost;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;


#define MANAGER_PORT_BASE 4000
#define BACKEND_SERVER_PORT_BASE 6900

const int AGING_INTERVAL_IN_SEC = 5; // Beat interval

void *precopyThread(void* p);
void *precopySendThread(void* p);
void *rewriteThread(void *p);
void *splitThread(void *p);
void *mergeThread(void *p);
void *heartbeatThreadLoop(void *p);
void *statsThreadLoop(void *p);
void *localConsumerThreadLoop(void *p);
void *localServerThreadLoop(void *p);
void *localDeadLoop(void *p);
void *localGetThreadLoop(void *p);
void *localGetResponseThreadLoop(void *p);

// You need this lock to protect access to 'h'
typedef struct interval_database {
    FawnDS<FawnDS_Flash>* h;
    FawnDS<FawnDS_Flash>* tempDS;
    string name;

    deque< pair< uint32_t,pair<string,string> > > seq_msg_queue; // seq -> <key, value>
    deque< pair< uint32_t,uint64_t > > seq_cont_queue; // seq -> contiuation
    deque< pair< uint32_t,pair<string,string> > > seq_msg_queue_tmp; // seq -> <key, value>
    deque< pair< uint32_t,uint64_t > > seq_cont_queue_tmp; // seq -> contiuation
    string splitPoint;
    uint32_t largest_seq;
    uint32_t last_ack;
    uint32_t last_put;

    atomic<uint32_t> sequence;
    atomic<bool> valid;
    string vid; // virtual id associated with this range

    pthread_rwlock_t dbLock;
} interval_db;

typedef struct splitMessage {
    void* p_node_mgr;
    string* key;
} splitMsg;

typedef struct rewriteMessage {
    void* p_node_mgr;
    interval_db* p;
} rewriteMsg;

typedef struct precopyMessage {
    void* p_node_mgr;
    string* startKey;
    string* endKey;
    string* vid;
    string* ip;
    int32_t port;
} precopyMsg;

class GetRequestObj {
public:
    const string key;
    int64_t continuation;
    string ip;
    GetRequestObj(string key, int64_t continuation, string ip) :
        key(key), continuation(continuation), ip(ip) {}
};

class GetResponseObj {
public:
    const string key;
    int64_t continuation;
    string value;
    returnStatus status;
    string ip;
    GetResponseObj(string key, int64_t continuation) :
            key(key), continuation(continuation) {}
};

class node_mgr : virtual public FawnKVBackendIf {
private:
    shared_ptr<TTransport> fe_transport;
    shared_ptr<TTransport> manager_transport;
    vector<interval_db*> dbs;
    bool overwrite;
    Ring *ring;

    pthread_t heartbeatThreadId_;
    bool b_hb;

    pthread_mutex_t count_mutex;
    pthread_cond_t count_threshold_cv;

    atomic<uint32_t> num_puts, num_gets, num_put_ws, total_num_puts, total_num_gets, total_num_put_ws;
    struct timeval *p_start, *p_end;
    fstream stat_file;
    uint32_t time;
    bool start_time;

#if defined(TBB_INTERFACE_VERSION) && (TBB_INTERFACE_VERSION >= 4001)
#define CQTYPE concurrent_bounded_queue
#else
#define CQTYPE concurrent_queue
#endif
    CQTYPE<GetRequestObj*> get_queue;
    //CQTYPE<GetResponseObj*> get_resp_queue;

    // Maps for holding timestamp values
    map<uint32_t, uint64_t> msgid_producer_map;
    map<uint32_t, uint64_t> msgid_consumer_map;

    map<string, atomic<uint32_t> > vid_precopy_map;
    map<string, int32_t > vid_extension_map;
    vector<string> m_VnodeIDs;
    uint32_t curr_vnode;

    map<string, int64_t> version_map;

    map< string, pair<string, string> > vid_sid_succIP_map;
    map< string, pair<string, string> > vid_pid_predIP_map;
    map<string, FawnKVBackendClient*> ip_client_map;
    map<string, FawnKVFrontendClient*> ip_fe_map;
    map<string, pthread_mutex_t*> ip_socketlock_map;
    FawnKVBackendClient* connectTCP(string IP, int port);
    FawnKVFrontendClient* connectTCP_FE(string IP, int port);
    void cleanup_sockets();

    string findVID(const string& key);

    pthread_rwlock_t interval_lock;


    void print_neighbours();
    void print_neighbours(const std::string& vid);

    // Consumers

    void static_join_response(const std::vector<std::string> & VnodeIDs, const std::string& ip);
    void rejoin_response(const std::vector<std::string> & VnodeIDs, const std::string& ip);
    void join_response(const std::vector<std::string> & VnodeIDs, const std::vector<std::string> & StartIDs, const std::string& ip);
    void init_response(const std::vector<std::string> & VnodeIDs, const std::string& ip);
    void vnode_pre_join_response(const std::string& VnodeID, const std::vector<std::string> & start_keys, const std::vector<std::string> & end_keys, const std::vector<std::string> & tail_ips, const std::vector<int32_t> & tail_ports, const std::string& ip);
    void precopy_request(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip, const int32_t port);
    void precopy_response(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip);
    void flush_split(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& joiner_ip, const std::vector<std::string> & forwarding_ips, const std::vector<std::string> & neighbor_ips, const std::vector<std::string> & neighbor_vids, const int32_t hops, const std::vector<int32_t> & forwarding_ports, const std::vector<int32_t> & neighbor_ports, const std::string& ip);
    void flush(const std::string&, const std::string&, int32_t);
    void flush_merge(const std::string& startKey, const std::string& endKey,
                     const std::string& merge_start_key, const std::string& merge_end_key,
                     const int32_t hops);
    void put_helper(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip, interval_db* i, int64_t version);
    void put(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip);
    void put_w(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip);
    void put_ack(const std::string& key, const int64_t seq, const int32_t hops, const std::string& ip);
    void get(const std::string& key, const int64_t continuation, const std::string& ip);
    void neighbor_update(const std::string& myvid, const std::vector<std::string> & pred_vids, const std::string& succ_vid, const std::string& succid, const std::string& pred_vid, const std::string& predid, const std::string& ip);
    void heartbeat_response(const std::string& ip);

    void chain_repair_tail(const std::string& key, const int32_t hops, const int32_t ackhops, const std::string& sid, const std::string& ip);
    void chain_repair_mid(const std::string& key,const std::string& my_vid, const std::string& sid, const std::string& ip, const int32_t hops, const int32_t ack_hops);
    void chain_repair_mid_succ( repairInfo& ret, const std::string& key, const std::string& p_vid, const std::string& p_ip);
    void chain_repair_single(const std::string& new_start, const std::string& old_start );

    void integrity(int32_t hops);

    void init(string managerIP, string localIP, string stat_filename);
    void get_handler(const std::string& key, const int64_t continuation, const std::string& ip);
    void vnode_extend_chain( const std::string& vid,const std::string& nid,  const std::string& start_key, const std::string& end_key, const std::string& ip, int32_t port, flushmode mode);

    void init(const string& ip, int32_t port);

public:
    FawnKVManagerClient *manager;
    TThreadedServer *server;
    uint32_t numThreads;
    string myIP;
    int myPort;
    string filebase;
    uint32_t num_precopies;
    bool unknown_ids;
    atomic<uint32_t> num_network;
    CQTYPE<GetResponseObj*> get_resp_queue;
    pthread_mutex_t socket_lock;

    // lockDB semantics: 0-don't lock,  1-read lock, 2-write lock
    enum {NOLOCK, RDLOCK, WRLOCK};
    interval_db* findIntervalDb(const string* key, int lockDB);

    //Function to deal with keyspaces that aren't merged yet. --jhferris 2/21/10
    list<interval_db*> findAllIntervalDb(const string* startKey, const string* endKey, int lockDB);

    node_mgr(string managerIP, string localIP, string stat_filename,
             bool overwriteDS);
    node_mgr(string managerIP, string localIP, string stat_filename,
             const list<string>* p_vids, bool overwriteDS);
    node_mgr();
    ~node_mgr();

    /* Control */
    int sendStaticJoinRequest();
    int sendJoinRequest();
    int sendReJoinRequest();

    /* Data  */
    void createIntervalFiles(const list<string>* p_vids, const list<string>* p_sids, const string my_vid = "");
    void openIntervalFiles(const list<string>* p_vids, const string my_vid = "");

    void create_interval_file(const string ps_endid, const string ps_startid, const string my_vid = "");

    interval_db* init_interval_db(const string endid, const string my_vid, const string startid = "");
    FawnDS<FawnDS_Flash>* createTempStore(const string& startid_str, const string& endid_str, const string& prefix_str);
    void insertIntervalFile(interval_db* i);
    void removeIntervalFile(interval_db* i);

    void startConsumerThread();
    void getConsumer();
    void getResponseConsumer();

    int cleanup();
    void close_files();

    // Stats
    void log_stats();

    FawnKVFrontendClient* connect_to_frontend(const string& nip);
    FawnKVBackendClient* connect_to_backend(const string& vid, const string& nid, const string& nip, bool bpred);
    FawnKVBackendClient* get_pred_client(const string& vid);
    FawnKVBackendClient* get_successor_client(const string& vid);

    static void sighandler(int sig);
    static bool b_done;

    // Functions that are run from within pthread_create. Might be unnecessary if thrift threadpool works as expected.
    void handle_split(const string& key);
    void precopy_request_thread(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip, const int32_t port);

    void print_all_db_ranges();

};

#endif // _NODE_MANAGER_H_
