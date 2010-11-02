/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

//needed to get INT64_MAX
#define __STDC_LIMIT_MACROS
#include <stdint.h>

#include "node_mgr.h"
#include "print.h"
#include "FawnKVBackend.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace fawn;

#define String(x) bytes_to_hex(x)
#define StringID(x) bytes_to_hex((x)->actual_data_str())
//#define SPLIT_DB

void node_mgr::integrity(int32_t hops)
{
    FawnKVBackendClient *succ = get_successor_client(m_VnodeIDs[0]);
    string nip = vid_sid_succIP_map[m_VnodeIDs[0]].second;
    cout << "Integrity message, hops are" << hops << endl;
    if(hops > 0) {
        pthread_mutex_lock(ip_socketlock_map[nip]);
        succ->integrity(hops-1);
        pthread_mutex_unlock(ip_socketlock_map[nip]);
    }
}

void node_mgr::static_join_response(const std::vector<std::string> & VnodeIDs, const std::string& ip) {
    printf("static_join_response\n");
    for (uint i = 0; i < VnodeIDs.size(); i++) {
        cout << "Virtual Node ID" << endl;
        print_payload(VnodeIDs[i]);
        cout << "-----" << endl;
    }
    m_VnodeIDs = VnodeIDs;
}

void node_mgr::rejoin_response(const std::vector<std::string> & VnodeIDs, const std::string& ip) {
    cout << "Rejoined Ring" << endl;
}

void node_mgr::join_response(const std::vector<std::string> & VnodeIDs, const std::vector<std::string> & StartIDs, const std::string& ip) {
    list<string> p_vids, p_sids;

    for (uint i = 0; i < VnodeIDs.size(); i++) {
        cout << "Virtual Node ID" << endl;
        string vid = VnodeIDs[i];
        p_vids.push_back(vid);
        string sid = StartIDs[i];
        p_sids.push_back(sid);
        print_payload(vid);
        cout << "Starting ID" << endl;
        print_payload(sid);
        cout << "-----" << endl;
    }
    printf("Creating Intervals...\n");

    createIntervalFiles(&p_vids, &p_sids);
}

void node_mgr::init_response(const std::vector<std::string> & VnodeIDs, const std::string& ip) {
    for (uint i = 0; i < VnodeIDs.size(); i++) {
        cout << "Virtual Node ID" << endl;
        print_payload(VnodeIDs[i]);
        cout << "-----" << endl;
    }

    m_VnodeIDs = VnodeIDs;
    pthread_mutex_lock(&socket_lock);
    manager->vnode_pre_join(m_VnodeIDs[curr_vnode], myIP, myPort);
    pthread_mutex_unlock(&socket_lock);
    curr_vnode++;
}

void node_mgr::vnode_pre_join_response(const std::string& VnodeID, const std::vector<std::string> & start_keys, const std::vector<std::string> & end_keys, const std::vector<std::string> & tail_ips, const std::vector<int32_t> & tail_ports, const std::string& ip) {

    if (tail_ips.size() == 0) { //first node joining
        string startKey = start_keys[0];
        string endKey = end_keys[0];
        create_interval_file(endKey, startKey, VnodeID);

        // Create temporary fawnds store for updates before flush finished
        FawnDS<FawnDS_Flash> *fawndstemp = createTempStore(startKey, endKey, "temp_");
        // Get current interval_db, set temporary db
        interval_db *i = findIntervalDb(&endKey, NOLOCK);
        assert(i != NULL);
        assert(i->tempDS == NULL);
        i->tempDS = fawndstemp;
        pthread_mutex_lock(&socket_lock);
        manager->vnode_join(VnodeID, myIP, myPort, false);
        pthread_mutex_unlock(&socket_lock);
    }
    else {
        num_precopies = tail_ips.size();
        vid_precopy_map[VnodeID] = 0;
        // For each tailIP: Send a precopy request to tailIP.
        for (uint i = 0; i < tail_ips.size(); i++) {
            string tailIP = tail_ips[i];
            int tailPort = tail_ports[i];
            string startKey = start_keys[i];
            vid_extension_map[startKey] = false;
            string endKey = end_keys[i];
            create_interval_file(endKey, startKey, VnodeID);

            cout << "StartKey: " << bytes_to_hex(startKey) << endl;
            cout << "EndKey: " << bytes_to_hex(endKey) << endl;

            cout << "Sending precopy request to " << tailIP << ":" << tailPort << endl;

            FawnKVBackendClient *precopyClient = connectTCP(tailIP, tailPort);
            precopyClient->precopy_request(startKey, endKey, VnodeID, myIP, myPort);
            //delete precopyClient;

        }
    }
}

void node_mgr::vnode_extend_chain( const std::string& vid, const std::string& nid, const std::string& start_key, const std::string& end_key, const std::string& ip, int32_t port, int32_t mode)
{
    cout << "Extend Chain for range [" << bytes_to_hex(start_key) << ", " << bytes_to_hex(end_key) << "]" << endl;
    cout << "Vid key is " << bytes_to_hex(vid) << endl;
    vid_precopy_map[vid] = 0;
    vid_extension_map[start_key] = mode;
    create_interval_file(end_key, start_key, vid);
    string ipport = getID(ip, port);
    FawnKVBackendClient *precopyClient = connect_to_backend(vid, nid, ipport, true);
    cout << "Sending precopy request to " << ipport << endl;
    pthread_mutex_lock(ip_socketlock_map[ipport]);
    precopyClient->precopy_request(start_key, end_key, vid, myIP, myPort);
    pthread_mutex_unlock(ip_socketlock_map[ipport]);
    //delete precopyClient;
}

void node_mgr::precopy_request(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip, const int32_t port) {
    cout << "[Precopy Request]" << endl;
    precopyMsg *pm = (precopyMsg*) malloc(sizeof(precopyMsg));
    pm->p_node_mgr = this;
    pm->startKey = new string(startKey);
    pm->endKey = new string(endKey);
    pm->vid = new string(vid);
    pm->ip = new string(ip);
    pm->port = port;

    pthread_t precopyThreadId_;
    pthread_create(&precopyThreadId_, NULL, precopyThread, pm);
}

void node_mgr::precopy_request_thread(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip, const int32_t port) {
    cout << "[PCRT] Connecting to " << ip << ":" << port << endl;

    // Setup socket to other destination server
    FawnKVBackendClient *backend = connectTCP(ip, port);

    DBID startID(startKey);
    DBID endID(endKey);

    /* Find interval_db that contains the split point */
    // Does not lock the db.

    interval_db* db;
    //CHANGEME
#ifdef SPLIT_DB
    list<interval_db*> db_list = findAllIntervalDb(&startKey, &endKey, NOLOCK);
    list<interval_db*>::iterator iter;
#else
    db = findIntervalDb(&endKey, NOLOCK);
#endif

#ifdef SPLIT_DB
    for (iter = db_list.begin(); iter!= db_list.end(); iter++)
    {
        db = *iter;
#endif
        if (db != NULL) {
            FawnDS<FawnDS_Flash> *h = db->h;
            char key[DBID_LENGTH];
            uint32_t key_length;
            bool valid;
            bool remove;
            string data_returned;

            // Reuse send_fm object, because we can.
            // setup split, keep track of IP for later flushing to this node after precopy
            h->split_init(getID(ip, port));
            cout << "Starting split" << endl;
            while (h->split_next(&startID, &endID, key, key_length, data_returned, valid, remove)) {
                //marshall data and send on socket.
                if (!valid) {
                    continue;
                }

                string s_key(key, key_length);
                backend->put(s_key, data_returned, 0, 0, 0, 0, true, remove, myIP);

                data_returned.clear();
            }
            // done!
            // New node takes over [startID,endID] and current DB is
            // [endID,currEndID]. But don't set the StartID until all data
            // is flushed to the joining node.


        } else {
            /* Error, hash not found */
            cout << "Error: key does not lie in the DB ranges owned by this node!" << endl;
        }
#ifdef SPLIT_DB
    }
#endif

    stat_file << "Ending precopy" << endl;
    cout << "Done sending precopy files." << endl;

    backend->precopy_response(startKey, endKey, vid, myIP);
    //delete backend;
}

void node_mgr::precopy_response(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& ip) {
#ifdef SPLIT_DB
    list<interval_db*> db_list = findAllIntervalDb(&startKey, &endKey, NOLOCK);
    list<interval_db*>::iterator iter;
    interval_db* db;
    for (iter = db_list.begin(); iter != db_list.end(); iter++)
    {
        db = *iter;
        FawnDS<FawnDS_Flash> *fawndstemp = createTempStore(startKey, endKey, "temp_");
        db->tempDS = fawndstemp;
    }
#else
    FawnDS<FawnDS_Flash> *fawndstemp = createTempStore(startKey, endKey, "temp_");
    // Get current interval_db, set temporary db
    interval_db *i = findIntervalDb(&endKey, NOLOCK);
    assert(i != NULL);
    assert(i->tempDS == NULL);
    i->tempDS = fawndstemp;

#endif
    // Will get a precopy response for each range of this vnodeID.
    vid_precopy_map[vid]++; // atomically increment
    cout << "vnode -- " << bytes_to_hex(vid.data()) << endl;
    cout << "got " << vid_precopy_map[vid] << " precopy responses." << endl;
    if (vid_extension_map[startKey] != flushmode::SPLIT)
    {
        cout << "[Precopy Response] Calling vnode_join with non-splitting flag" << endl;
        pthread_mutex_lock(&socket_lock);
        manager->vnode_join(startKey, myIP, myPort, vid_extension_map[startKey]);
        pthread_mutex_unlock(&socket_lock);
    }
    if (vid_precopy_map[vid] == num_precopies) {
        vid_precopy_map[vid] = 0;
        cout << "Sending vnode join" << endl;
        stat_file << "Sending vnode join" << endl;
        // db ready, send official join
        pthread_mutex_lock(&socket_lock);
        manager->vnode_join(vid, myIP, myPort, false);
        pthread_mutex_unlock(&socket_lock);
    }
}

void node_mgr::flush_split(const std::string& startKey, const std::string& endKey, const std::string& vid, const std::string& joiner_ip, const std::vector<std::string> & forwarding_ips, const std::vector<std::string> & neighbor_ips, const std::vector<std::string> & neighbor_vids, const int32_t hops, const std::vector<int32_t> & forwarding_ports, const std::vector<int32_t> & neighbor_ports, const std::string& ip) {
    interval_db *i = NULL;
    FawnDS<FawnDS_Flash> *h = NULL;
    FawnKVBackendClient *precopy_backend = NULL;

    cout << "----------" << endl;
    cout << "StartKey: " << bytes_to_hex(startKey) << endl;
    cout << "EndKey: " << bytes_to_hex(endKey) << endl;
    cout << "VID: " << bytes_to_hex(vid) << endl;
    cout << "Hops: " << hops << endl;

    // Action taken depends on hop count of message. hops starts at L+2 (L = chain length) and decrements.
    switch ( hops ) {
    case 0:
        // Invalid, only front-end should get this as a confirmation that CMM got through.
        cerr << "Error in flushConsumer: only front-end should receive CMM with hops = 0." << endl;
        break;
    case 1:
    {
        // Indication to joining node that flush is complete.  Begin merge of temp datastore
        // XXX TODO for multiple vnodes: start sending next vnode join request if it exists.
        cout << "CMM Hops = 1" << endl;
#ifdef SPLIT_DB
        list<interval_db*> db_list = findAllIntervalDb(&startKey, &endKey, NOLOCK);
        list<interval_db*>::iterator iter;
        interval_db* db;
        for (iter = db_list.begin(); iter != db_list.end(); iter++)
        {
            db = *iter;
            h = db->h;
            FawnDS<FawnDS_Flash> *tempDS = db->tempDS;
            // Can do merge later if needed.
            bool b = h->Merge(tempDS, NULL);
            if (!b) {
                cerr << "Error merging temporary store into main store!" << endl;
            }
            db->tempDS = NULL;
        }
        if (db_list.size() == 0)
        {
            cout << "Error: key does not lie in the DB ranges owned by this node!" << endl;
        }

#else
        //CHANGEME
        interval_db *db = findIntervalDb(&endKey, NOLOCK);
        if (db != NULL) {
            // temporary database should exist.
            assert(db->tempDS != NULL);
            h = db->h;
            FawnDS<FawnDS_Flash> *tempDS = db->tempDS;
            // Can do merge later if needed.
            bool b = h->Merge(tempDS, NULL);
            if (!b) {
                cerr << "Error merging temporary store into main store!" << endl;
            }
            db->tempDS = NULL;
        } else {
            // Error, hash not found
            cout << "Error: key does not lie in the DB ranges owned by this node!" << endl;
        }
#endif

        if (curr_vnode < m_VnodeIDs.size()) {
            pthread_mutex_lock(&socket_lock);
            manager->vnode_pre_join(m_VnodeIDs[curr_vnode], myIP, myPort);
            pthread_mutex_unlock(&socket_lock);
            curr_vnode++;
        }

        break;
    }
    case 2:
    {
        // Old Tail -- flush the rest of its DS from currSplit to joining node
        // Reception of CMM means that it won't get any more updates to this DS.
        cout << "CMM Hops = 2" << endl;

        DBID startID(startKey);
        DBID endID(endKey);

        // Find appropriate intervaldb
        // Does not lock the db.
        //CHANGEME
        interval_db *db = findIntervalDb(&endKey, NOLOCK);

        if (db != NULL) {
            h = db->h;
            i = db;
            char key[DBID_LENGTH];
            uint32_t key_length;
            //char *data_returned = NULL;
            bool valid;
            bool remove;
            string data_returned;
            assert(h != NULL);


            cout << "Connecting to " << h->precopyIP << endl;
            precopy_backend = connectTCP(getIP(h->precopyIP), getPort(h->precopyIP));

            // continue from currSplit.
            while (h->split_next(&startID, &endID, key, key_length, data_returned, valid, remove)) {
                //marshall data and send on socket.
                if (!valid) {
                    continue;
                }

                string s_key(key, key_length);
                precopy_backend->put(s_key, data_returned, 0, 0, 0, 0, true, remove, myIP);

                data_returned.clear();
            }
            stat_file << "Finished final flush" << endl;
            // done with transfer!

            // optionally, start rewrite function to prune unused data items immediately.

        } else {
            // Error, hash not found
            cout << "Error: key does not lie in the DB ranges owned by this node!" << endl;
        }


        break;
    }
    default:
        cout << "CMM Hops = " << hops << endl;
        // Default case: do nothing.
        break;
    }

    // Find current neighbor socket for this vid
    //int successorSocket = get_successor_sock(vid);
    FawnKVBackendClient *next_hop = NULL;
    pthread_mutex_t *next_hop_socketlock = NULL;

    // Update neighbor as long as hops > 2 --- if hops <= 2, no neighbors need to be updated.
    if (hops > 2) {

        cout << "Neighbor IP list:" << endl;
        for (uint i = 0; i < neighbor_ips.size(); i++) {
            string hop_ip = neighbor_ips[i];
            int32_t hop_port = neighbor_ports[i];
            cout << "ip: " << hop_ip << ":" << hop_port << endl;
        }

        cout << "Forwarding IP list:" << endl;
        for (uint i = 0; i < forwarding_ips.size(); i++) {
            string hop_ip = forwarding_ips[i];
            int32_t hop_port = forwarding_ports[i];
            cout << "ip: " << hop_ip << ":" << hop_port << endl;
        }
        string my_vid;
        for (uint i = 0; i < neighbor_ips.size() - 1; i++) {
            string hop_ip = neighbor_ips[i];
            int32_t hop_port = neighbor_ports[i];
            if ((hop_ip == myIP) && (hop_port == myPort)) {
                string new_neighbor_ip = neighbor_ips[i+1];
                int32_t new_neighbor_port = neighbor_ports[i+1];
                string new_vid = neighbor_vids[i+1];
                my_vid = neighbor_vids[i];
                cout << "my ip: " << hop_ip << ", neighbor ip = " << new_neighbor_ip << endl;
                cout << "Trying to connect to " << new_neighbor_ip << ":" << new_neighbor_port << endl;

                FawnKVBackendClient *tempClient = NULL;
                string nid = getID(new_neighbor_ip, new_neighbor_port);
                tempClient = connect_to_backend(my_vid, new_vid, nid, false);
                assert(tempClient != NULL);

                if (i > 0) {
                    string pred_ip = neighbor_ips[i-1];
                    string new_pred = neighbor_vids[i-1];
                    int32_t pred_port = neighbor_ports[i-1];
                    cout << "Trying to connect to " << pred_ip << ":" << pred_port << endl;
                    nid = getID(pred_ip, pred_port);
                    tempClient = connect_to_backend(my_vid, new_pred, nid, true);
                    assert(tempClient != NULL);
                }

                break;
            }

        }

        if (endKey == vid && vid != my_vid) {
            cout << "myvid = " << my_vid << endl;
            cout << endKey << " == " << vid << ", splitting " << endl;
            // This range logically needs to be split
            // get i (lookup splitPoint)
            //CHANGEME
            interval_db *i = findIntervalDb(&endKey, WRLOCK);
            // set i->splitPoint to endKey
            i->splitPoint = endKey;
            pthread_rwlock_unlock(&(i->dbLock));
            // start split of db
            splitMsg *sm = (splitMsg*) malloc(sizeof(splitMsg));
            sm->p_node_mgr = this;
            sm->key = new string(endKey);
            pthread_t splitThreadId_;
            pthread_create(&splitThreadId_, NULL, splitThread, sm);
        }

        // TODO get port for forwarding IPs
        string next_ip = forwarding_ips[forwarding_ips.size() - hops + 1];
        int32_t next_port = forwarding_ports[forwarding_ports.size() - hops + 1];
        cout << "forwarding neighbor = " << next_ip << ":" << next_port << endl;
        // Creating empty mapping to ensure TCP connection is made to the neighbor.
        // (This is just an easy way to get the mapping)
        string temps = "";
        string nid = getID(next_ip, next_port);
        next_hop = connect_to_backend(temps, temps, nid, true);
        next_hop_socketlock = ip_socketlock_map[nid];
    }


    // Forward CMM message with decremented number of hops to the appropriate person.
    // Decrement number of hops and send
    if (hops == 2) {
        // forward message back to joining node
        precopy_backend->flush_split(startKey, endKey, vid, joiner_ip, forwarding_ips, neighbor_ips, neighbor_vids, hops-1, forwarding_ports, neighbor_ports, myIP);
        //delete precopy_backend;
    }
    else if (hops == 1) {
        // forward message to FE
        pthread_mutex_lock(&socket_lock);
        manager->flush_split(startKey, endKey, vid, joiner_ip, forwarding_ips, neighbor_ips, neighbor_vids, hops-1, myIP);
        pthread_mutex_unlock(&socket_lock);
    } else {
        // forward message
        pthread_mutex_lock(next_hop_socketlock);
        next_hop->flush_split(startKey, endKey, vid, joiner_ip, forwarding_ips, neighbor_ips, neighbor_vids, hops-1, forwarding_ports, neighbor_ports, myIP);
        pthread_mutex_unlock(next_hop_socketlock);
    }

    // If old tail, close socket after forwarding CMM
    // Also, set the range on FawnDS appropriately.
    if (hops == 2) {
        //TODO close socket?
        cout << "Old tail finished, cleaning up" << endl;
        assert(h != NULL);
        assert(i != NULL);
        DBID startID(endKey);
        // Need lock on h to make sure startID is read atomically
        pthread_rwlock_wrlock(&(i->dbLock));
        h->setStartID(startID);
        pthread_rwlock_unlock(&(i->dbLock));
    }

    cout << "----------" << endl;
    print_neighbours();

}


/**
 * No splitting or merging, just flush down
 */
void node_mgr::flush(const std::string& startKey, const std::string& endKey, const int32_t hops) {

    cout << "[Flush] hops is " << hops << endl;
    cout << "Range is [" << String(startKey) << ", " << String(endKey) << "]" << endl;

    interval_db *i;
    i = findIntervalDb(&endKey, NOLOCK);
    FawnKVBackendClient *succ = get_successor_client(i->vid);
    string nip = vid_sid_succIP_map[i->vid].second;

    const DBID *di_end_id = i->h->getEndID();
    const DBID *di_start_id = i->h->getStartID();

    if (hops > 1)
    {
        pthread_mutex_lock(ip_socketlock_map[nip]);
        succ->flush(startKey, endKey, hops-1);
        pthread_mutex_unlock(ip_socketlock_map[nip]);
    }
    else if (hops == 1) {
      cout << "[Flush] old tail; flushing ..."  << endl;
      string data_returned;
      char key[DBID_LENGTH];
      uint32_t key_length;
      bool valid;
      bool remove;
      // continue from currSplit.
      while (i->h->split_next(di_start_id, di_end_id, key, key_length, data_returned, valid, remove))
      {
        //marshall data and send on socket.
        if (!valid) {
          continue;
        }
        string s_key(key, key_length);
        pthread_mutex_lock(ip_socketlock_map[nip]);
        succ->put(s_key, data_returned, 0, 0, 0, 0, true, remove, myIP);
        pthread_mutex_unlock(ip_socketlock_map[nip]);
        data_returned.clear();
      }
      //send to new tail
      cout << "[Flush] Passing along to new tail " ;
      cout << vid_sid_succIP_map[i->vid].second  << " and hops is " << hops << endl;
      pthread_mutex_lock(ip_socketlock_map[nip]);
      succ->flush(startKey, endKey, hops-1);
      pthread_mutex_unlock(ip_socketlock_map[nip]);
      cout << "Message Sent" << endl;

    }
    else if (hops == 0)
    {
        // temporary database should exist.
        assert(i->tempDS != NULL);
        FawnDS<FawnDS_Flash> *tempDS = i->tempDS;
        // Can do merge later if needed.
        bool b = i->h->Merge(tempDS, NULL);
        if (!b) {
            cerr << "Error merging temporary store into main store!" << endl;
        }
        i->tempDS = NULL;
        pthread_mutex_lock(&socket_lock);
        manager->flush_nosplit(startKey);
        pthread_mutex_unlock(&socket_lock);
    }

    delete di_end_id;
    delete di_start_id;
}

/** We want to do 2 things:
 *
 * 1. merge range [merge_start_key, merge_end_key] into [startKey, endkey]
 * 2. if necessary, pass on the message
 *
 * Assumption: startKey > m_start
 *
 * m_start < startKey < endKey
 */

void node_mgr::flush_merge(
    const std::string& merge_start_key, const std::string& merge_end_key,
    const std::string& startKey, const std::string& endKey,
    const int32_t hops) {

    interval_db *i,*j;


    cout << "[Flush merge] hops is " << hops << endl;
    cout << "First Range is [" << String(merge_start_key) << ", " << String(merge_end_key) << "]" << endl;
    cout << "Second Range is [" << String(startKey) << ", " << String(endKey) << "]" << endl;
    cout << "merge range's start is " << String(merge_start_key) << endl;

    //get the 2 databases
    i = findIntervalDb(&endKey, NOLOCK);
    j = findIntervalDb(&merge_end_key, NOLOCK);
    const DBID *di_end_id = i->h->getEndID();
    const DBID *di_start_id = i->h->getStartID();
    const DBID *dj_end_id = j->h->getEndID();
    const DBID *dj_start_id = j->h->getStartID();

    cout << "I is [" << StringID( (const DBID*)di_start_id ) << " , " << StringID( (const DBID*)di_end_id ) << "]\n";
    cout << "J is [" << StringID( (const DBID*)dj_start_id ) << " , " << StringID( (const DBID*)dj_end_id ) << "]\n";
    //invalidate the one we're going to drop
    j->valid = false;

    //at some point this is going to be a "mark for merge" or something similar.
    bool b = i->h->Merge(j->h, NULL);

    //Expand the range of the merged db
    i->h->setStartID(DBID(merge_start_key));

    //get successor for passing along message
    FawnKVBackendClient *succ = get_successor_client(i->vid);

    //we are neither the old tail or the new tail, we've already merged, just pass along
    if (hops > 1)
    {
        cout << "[Flush merge] Passing along..." ;
        cout << "to " << vid_sid_succIP_map[i->vid].second  << " and hops is " << hops << endl;
        succ->flush_merge(merge_start_key, merge_end_key, startKey, endKey, hops-1);
        cout << "Message Sent" << endl;
    }
    //old tail, needs to flush what it has
    else if (hops == 1)
    {
        cout << "[Flush merge] old tail; flushing ..."  << endl;
        string data_returned;
        char key[DBID_LENGTH];
        uint32_t key_length;
        bool valid;
        bool remove;
        // continue from currSplit.
        while (j->h->split_next(dj_start_id, dj_end_id, key, key_length, data_returned, valid, remove))
        {
            //marshall data and send on socket.
            if (!valid) {
                continue;
            }
            string s_key(key, key_length);
            succ->put(s_key, data_returned, 0, 0, 0, 0, true, remove, myIP);
            data_returned.clear();
        }
        //send to new tail
        cout << "[Flush merge] Passing along to new tail " ;
        cout << vid_sid_succIP_map[i->vid].second  << " and hops is " << hops << endl;
        succ->flush_merge(merge_start_key, merge_end_key, startKey,endKey, hops-1);
        cout << "Message Sent" << endl;
    }
    //new tail case
    else if (hops == 0)
    {
        cout << "[Flush merge] new tail" << endl;
        // temporary database should exist.
        assert(j->tempDS != NULL);
        FawnDS<FawnDS_Flash> *tempDS = j->tempDS;
        bool b = i->h->Merge(tempDS, NULL);
        if (!b) {
            cerr << "Error merging temporary store into main store!" << endl;
        }
        j->tempDS = NULL;
        //forward the flush to the frontent
        pthread_mutex_lock(&socket_lock);
        manager->flush_nosplit(merge_start_key);
        pthread_mutex_unlock(&socket_lock);
    }

    delete di_end_id;
    delete di_start_id;
    delete dj_end_id;
    delete dj_start_id;

//    pthread_rwlock_unlock(&(i->dbLock));
//    pthread_rwlock_unlock(&(j->dbLock));
    //should get rid of j at this point...

}

//if version is -1, then it's a normal put
//if version is >= 0, then it's a put_w
void node_mgr::put_helper(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip, interval_db* i, int64_t version) {

    // Create FE connection to IP if doesn't exist.
    FawnKVFrontendClient *fe = connect_to_frontend(ip);

    int32_t new_hops = hops-1;
    string owner_vid;
    bool rc = false;

    FawnDS<FawnDS_Flash> *h = NULL;
    // if temporary DS exists, put there.
    if (i != NULL) {
        if ( (i->tempDS == NULL) || flush )
            h = i->h;
        else
            h = i->tempDS;
    }
    if (h != NULL) {

        // Get my vid for the range that this put corresponds to
        owner_vid = i->vid;

        const DBID* d_sid = i->h->getStartID();
        const DBID* d_eid = i->h->getEndID();

        //cout << "   - [put] my vid = " << bytes_to_hex(owner_vid) << ", interval we're inserting into is [ " << bytes_to_hex(d_sid->actual_data_str()) << " , " << bytes_to_hex(d_eid->actual_data_str()) << " ] " << endl;

        delete d_sid;
        delete d_eid;

        if (remove)
            rc = h->Delete(key.data(), key.length());
        else
            rc = h->Insert(key.data(), key.length(), value.data(), value.size());

        i->last_put = seq;
        if (!rc && i->tempDS != NULL) {
            // Insert into base datastore if not successful.
            FawnDS<FawnDS_Flash> *baseDS = i->h;
            if (remove)
                rc = baseDS->Delete(key.data(), key.length());
            else
                rc = baseDS->Insert(key.data(), key.length(), value.data(), value.size());
        }

        if (!flush) {

           if (new_hops == 0) {
                // tail
                i->last_ack = seq;
            }
            else {
                // buffer <seq, <key,value>>
                pair<string,string> mesg(key, value);
                pair< uint32_t, pair<string,string> > p(seq, mesg);
                pair< uint32_t, uint64_t > q(seq, continuation);
                if (i->splitPoint == "") {
                    i->seq_msg_queue.push_back(p);
                    i->seq_cont_queue.push_back(q);
                }
                else
                {
                    i->seq_msg_queue_tmp.push_back(p);
                    i->seq_cont_queue_tmp.push_back(q);
                }
            }
        }

        //unlock original db
        pthread_rwlock_unlock(&(i->dbLock));

        if (rc != true) {
            cout << "Error inserting into database, code: " << rc << endl;
        }
    }
    else {
        cout << "Error: key does not lie in the DB ranges owned by this node!" << endl;
    }


    if (!rc) {
        // error case
        // send put response to front end immediately.  XXX is this right?
        pthread_mutex_lock(ip_socketlock_map[ip]);
        if (version != -1) {
            fe->put_w_response(key, continuation, rc, version, myIP);
        } else {
            fe->put_response(key, continuation, rc, myIP);
        }
        pthread_mutex_unlock(ip_socketlock_map[ip]);
    } else {
        if (!flush) {
            //      cout << "[PUT] " << key << ", seq " << seq << ", hops " << hops  << " db key: " << i->vid <<  endl;
            if (new_hops == 0) {
                // send response to front end
                pthread_mutex_lock(ip_socketlock_map[ip]);
                if (version != -1) {
                    fe->put_w_response(key, continuation, rc, version, myIP);
                } else {
                    fe->put_response(key, continuation, rc, myIP);
                }
                pthread_mutex_unlock(ip_socketlock_map[ip]);
                if (ackhops - 1 > 0) {
                    FawnKVBackendClient *pred = get_pred_client(owner_vid);
                    assert(pred != NULL);
                    string nip = vid_pid_predIP_map[owner_vid].second;
                    cout << "   - Sending PUT_ACK" << ", key is " << bytes_to_hex(key) << ", destination IP is..." << nip << endl;
                    pthread_mutex_lock(ip_socketlock_map[nip]);
                    try {
                        pred->put_ack(key, seq, ackhops - 2, myIP); // try, catch and ignore?
                    }
                    catch (...) {
                        string s;
//                        cin >> s;
                    }
                    pthread_mutex_unlock(ip_socketlock_map[nip]);
                }
            } else {
                FawnKVBackendClient *succ = get_successor_client(owner_vid);
                assert(succ != NULL);
                string nip = vid_sid_succIP_map[owner_vid].second;
                cout << "   - Forwarding Put to " << nip << endl;
                uint32_t sequence = seq;
                if (sequence == 0) {
                    // Assign sequence number
                    sequence = i->sequence.fetch_and_increment();
                }
                pthread_mutex_lock(ip_socketlock_map[nip]);
                succ->put(key, value, new_hops, ackhops, continuation, sequence, flush, remove, ip); // try, catch and ignore?
                pthread_mutex_unlock(ip_socketlock_map[nip]);
            }
        }
    }
}




void node_mgr::put(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip) {

    //cout << "[PUT] Key is " << bytes_to_hex(key) << ", Hop Count: " << hops << ", Ack Hop Count " << ackhops << ", seq is " << seq << endl;

    // db comes locked.
    interval_db *i = findIntervalDb(&key, WRLOCK);
    //cout << "   - [put] got lock" << endl;

    put_helper(key, value, hops, ackhops, continuation, seq, flush, remove, ip, i, -1);

    num_puts++;
    total_num_puts++;
}

// put_w is put for widekv
void node_mgr::put_w(const std::string& key, const std::string& value, const int32_t hops, const int32_t ackhops, const int64_t continuation, const int64_t seq, const bool flush, const bool remove, const std::string& ip) {

    // db comes locked.
    interval_db *i = findIntervalDb(&key, WRLOCK);
    
    // we need to keep multiple versions around, so for now I'm just
    // going to keep a map from key to the current version and then
    // I'll tell fawnds that the "key" is key::version

    // we can make this more efficient later (hash_map) if it becomes
    // a bottleneck, but I'd suspect we'd integrate with fawnds before
    // that happened anyway

    int64_t version = version_map[key]++;
    string full_key(key);
    full_key.append(":" + to_string(version));

    cout << "[PUTW] Key is " << bytes_to_hex(key) << ", version: " << version
         << ", Hop Count: " << hops << ", Ack Hop Count " << ackhops
         << ", seq is " << seq << endl;

    put_helper(full_key, value, hops, ackhops, continuation, seq, flush, remove, ip, i, version);

    num_put_ws++;
    total_num_put_ws++;
}

void node_mgr::put_ack(const std::string& key, const int64_t seq, const int32_t hops, const std::string& ip) {
    cout << "[PUTACK] seq = " << seq << ", hops " << hops << ", and key is " <<  bytes_to_hex(key) << endl;

    // db comes locked.
    interval_db *i = findIntervalDb(&key, WRLOCK);
    assert(i != NULL);

    i->last_ack = seq; //i->seq_msg_queue_tmp.front().first;
    bool inTempQueue = false;
    if (i->splitPoint != "") {
        while ( !(i->seq_msg_queue_tmp.empty()) &&
                ((i->seq_msg_queue_tmp).front().first <= seq) )
        {
            //    cout << "POP " << i->seq_msg_queue_tmp.front().first << endl;
            i->seq_msg_queue_tmp.pop_front();
            inTempQueue = true;
        }
    }

    if (!inTempQueue) {
        while ( !(i->seq_msg_queue.empty()) &&
                ((i->seq_msg_queue).front().first <= seq) )
        {
            i->last_ack = i->seq_msg_queue.front().first;
            //    cout << "POP " << i->seq_msg_queue.front().first << endl;
            i->seq_msg_queue.pop_front();
        }
    }

    if (hops > 0) {
        string my_vid = i->vid;
        FawnKVBackendClient *pred = get_pred_client(my_vid);
        string nip = vid_pid_predIP_map[i->vid].second;
        cout << "    - [putack] Forarding to " << nip << endl;

        assert(pred != NULL);
        pthread_mutex_lock(ip_socketlock_map[nip]);
        pred->put_ack(key, seq, hops - 1, myIP); // try, catch and ignore?
        pthread_mutex_unlock(ip_socketlock_map[nip]);
    }
    pthread_rwlock_unlock(&(i->dbLock));

}

void node_mgr::get(const std::string& key, const int64_t continuation, const std::string& ip) {
    //cout << "GET" << endl;
    get_queue.push( new GetRequestObj(key, continuation, ip) );
}

void node_mgr::getConsumer() {
    GetRequestObj *gr;

    // uses tbb concurrent queue: is thread safe.
    get_queue.pop(gr);
    get_handler(gr->key, gr->continuation, gr->ip);
    delete gr;
}


void node_mgr::get_handler(const std::string& key, const int64_t continuation, const std::string& ip) {
    // Get from FawnDS
    bool rc = false;

    // db comes locked
    interval_db *idb = findIntervalDb(&key, RDLOCK);
    FawnDS<FawnDS_Flash> *h = NULL;
    // if temporary DS exists, put there.
    if (idb != NULL) {
        if (idb->tempDS == NULL)
            h = idb->h;
        else
            h = idb->tempDS;
    }

    //msgid_consumer_map[fm->msgid()] = rdtsc();
    GetResponseObj *gr = new GetResponseObj(key, continuation);
    if (h != NULL) {
        rc = h->Get(key.data(), key.length(), gr->value);
        if (!rc && idb->tempDS != NULL) {
            // Check base datastore if not found in temp.
            FawnDS<FawnDS_Flash> *baseDS = idb->h;
            rc = baseDS->Get(key.data(), key.length(), gr->value);
        }

        //unlock original DB
        pthread_rwlock_unlock(&(idb->dbLock));

        gr->status = rc ? returnStatus::SUCCESS : returnStatus::NOT_EXIST;
        if (!rc) {
            gr->value.clear();
        }

        gr->ip = ip;
        get_resp_queue.push(gr);
    }
    else {
        cout << "Error GH: key does not lie in the DB ranges owned by this node!" << endl;
        gr->status = returnStatus::STALE_RING;
        gr->ip = ip;
        get_resp_queue.push(gr);
    }
}

void node_mgr::getResponseConsumer() {
    GetResponseObj *gr;
    // uses tbb concurrent queue: is thread safe.
    get_resp_queue.pop(gr);
    FawnKVFrontendClient *fe = connect_to_frontend(gr->ip);
    pthread_mutex_lock(ip_socketlock_map[gr->ip]);
    fe->get_response(gr->key, gr->value, gr->continuation, gr->status, myIP);
    pthread_mutex_unlock(ip_socketlock_map[gr->ip]);
    num_gets++;
    total_num_gets++;

    delete gr;
}


void node_mgr::chain_repair_single(const std::string& new_start , const std::string& endid)
{
    interval_db* db = findIntervalDb(&endid, NOLOCK);
    assert(db != NULL);
    const DBID* d_sid = db->h->getStartID();
    cout << "repair: extending range from [" << StringID( (const DBID*)d_sid ) << ", " << bytes_to_hex(endid)
         << "] to [" << bytes_to_hex(new_start) << ", " << bytes_to_hex(endid) << "]" << endl;
    db->h->setStartID(DBID(new_start));
    delete d_sid;
}

/**
 * key = key in the affected range = end_id (which is part of the range)
 **/
void node_mgr::chain_repair_tail(const std::string& key, const int32_t hops, const int32_t ackhops, const std::string& sid, const std::string& ip)
{
    // db comes locked.
    interval_db *i = findIntervalDb(&key, NOLOCK);
    assert(i != NULL);
    const DBID *di_end_id = i->h->getEndID();
    const DBID *di_start_id = i->h->getStartID();
    string s;
    cout << "In CR_T, key = " << bytes_to_hex(key) << ", Hops:" << hops<< endl;
    cout << "I is [" << StringID( (const DBID*)di_start_id ) << " , " << StringID( (const DBID*)di_end_id ) << "]\n";

    //we've reached the new tail and need to ack all pending.
    if (hops <= 0) {
        cout << "CR_T newtail, flushing acks" << endl;
        //using INT64_MAX to flush all pending puts.
        put_ack(key, INT64_MAX, ackhops - 1, myIP);
        //cout << "di_start_id: " << StringID(di_start_id) << endl;
        cout << "connecting to new successor " << bytes_to_hex(sid) << ", at " << ip << endl;
        FawnKVBackendClient* f = connect_to_backend(i->vid, sid, ip, false);
        pthread_mutex_lock(&socket_lock);
        manager->chain_repair_done(di_start_id->actual_data_str(), di_end_id->actual_data_str());
        pthread_mutex_unlock(&socket_lock);
    } else {
        cout << "CR_T sending along to " << endl;
        string nip = vid_sid_succIP_map[i->vid].second;
        cout << "to " << nip  << " and hops is " << hops << endl;
        FawnKVBackendClient *succ = get_successor_client(i->vid);
        assert(succ != NULL);
        pthread_mutex_lock(ip_socketlock_map[nip]);
        succ->chain_repair_tail(key, hops-1, ackhops, sid, ip);
        pthread_mutex_unlock(ip_socketlock_map[nip]);
    }
}

/**
 * key - representing interval_db
 * my_vid - vid of virtualnode we're "on"
 * sid - successor vid
 * ip - successor ip (now includes :port)
 * hops - number of hops forward for resending puts
 * ack_hops - number of hops forward for doing acks
 */
void node_mgr::chain_repair_mid(const std::string& key, const std::string& my_vid, const std::string& sid, const std::string& ip, const int32_t hops, const int32_t ack_hops)
{
    cout << "IN CR_M:";
    cout << "\t" << "Key: " << bytes_to_hex(key);
    cout << "\t" << "my_vid: " << bytes_to_hex(my_vid);
    cout << "\t" << "sid: " << bytes_to_hex(sid);
    cout << "\t" << "sip: " << ip <<endl;
    cout << "\t" << "hops: " << hops <<endl;
    cout << "\t" << "ack_hops: " << ack_hops <<endl;
    cout << "\t" << "MY_IP: " << myIP <<endl;
    FawnKVBackendClient* f = connect_to_backend(key, sid, ip, false);
    string nip = vid_sid_succIP_map[key].second;
    cout << "My successor for the range " << bytes_to_hex(key) << " is now " << nip << endl;
    repairInfo info;
    interval_db* db = findIntervalDb(&key, NOLOCK);
    assert(db != NULL);
    const DBID *di_end_id = db->h->getEndID();
    const DBID *di_start_id = db->h->getStartID();

    //needs to establish pred link and then give us the last_ack and last_put
    pthread_mutex_lock(ip_socketlock_map[nip]);
    f->chain_repair_mid_succ(info, key, my_vid, getID(myIP, myPort));
    pthread_mutex_unlock(ip_socketlock_map[nip]);
    cout << "Returned from CRM, got last_ack = " << info.last_ack << " and last put = " << info.last_put << endl;


    //collect any acks that we missed, if necessary
    if ( db->last_ack != (uint32_t) info.last_ack) {
        cout << "Their last was " << info.last_ack << " and ours was " << db->last_ack << endl;
        put_ack(key, info.last_ack, ack_hops, myIP);
    }

    /* now for the puts they missed
     *
     * Any puts they have yet to receive must be in our seq_msg_queue,
     * because we could not have received an ack for something they
     * havent even put yet. so we need to go through our queue and
     * re-put anything that is after their 'last put'
     **/

    deque< pair< uint32_t,pair<string,string> > >::iterator pending;
    deque< pair< uint32_t, uint64_t > >::iterator cont_pending;
    for(pending = db->seq_msg_queue.begin(), cont_pending = db->seq_cont_queue.begin();pending != db->seq_msg_queue.end();pending++, cont_pending++)
    {
        if ( (*pending).first > (uint32_t)info.last_put)
        {
            //I hope my math is right here for the number of ack_hops
            //what is continuation??
            //I think flush is right
            pthread_mutex_lock(ip_socketlock_map[nip]);
            f->put( (*pending).second.first,
                    (*pending).second.second,
                    hops, ack_hops+hops,
                    (*cont_pending).second,
                    (*pending).first, false,
                    false, myIP);  // XXX Putting false for remove flag, this is incorrect, fix.
            // Also, does myIP need to be replaced with the feIP associated with this obj since flush is false?
            pthread_mutex_unlock(ip_socketlock_map[nip]);
        }
    }

    pthread_mutex_lock(&socket_lock);
    manager->chain_repair_done(di_start_id->actual_data_str(), di_end_id->actual_data_str());
    pthread_mutex_unlock(&socket_lock);

}


//have to return both most recent ack and last put
// p_ip now includes :port
void node_mgr::chain_repair_mid_succ( repairInfo& ret,
                                      const std::string& key,
                                      const std::string& p_vid,
                                      const std::string& p_ip)
{
    cout << "IN CR_M_S:";
    cout << "\t" << "Key: " << bytes_to_hex( key);
    cout <<endl;
    cout << "\t" << "p_vid: " << bytes_to_hex(p_vid);
    cout << endl;
    cout << "\t" << "p_ip: " << p_ip <<endl;
    cout << "\t" << "MY_IP: " << myIP <<endl;

    interval_db* db = findIntervalDb(&key, NOLOCK);
    //establish back-connection
    FawnKVBackendClient* f = connect_to_backend(db->vid, p_vid, p_ip, true);
    cout << "My predecessor for the range " << bytes_to_hex(key) << " is now " << vid_pid_predIP_map[key].second << endl;


    if (db == NULL) {
        cout << "CRMS does have that db!!!";
    } else {
        ret.last_ack = db->last_ack;
        ret.last_put = db->last_put;
        cout << "Last ack is " << ret.last_ack << " and last_put is " << ret.last_put << endl;
    }
    return;
}

void node_mgr::neighbor_update(const std::string& myvid, const std::vector<std::string> & pred_vids, const std::string& succ_vid, const std::string& succip, const std::string& pred_vid, const std::string& predip, const std::string& ip) {
    cout << "my vid = " << myvid << endl;
    cout << "succ vid = " << bytes_to_hex(succ_vid) << ", succ IP = " << succip << endl;
    cout << "pred vid = " << bytes_to_hex(pred_vid) << ", pred IP = " << predip << endl;

    FawnKVBackendClient *temp = NULL;
    temp = connect_to_backend(myvid, succ_vid, succip, false);
    assert(temp != NULL);
    temp = connect_to_backend(myvid, pred_vid, predip, true);
    assert(temp != NULL);

    list<string> p_vids, p_sids;

    for (uint i = 0; i < pred_vids.size(); i++) {
        if (i != 0) {
            p_vids.push_back(pred_vids[i]);
        }
        p_sids.push_back(pred_vids[i]);
    }

    p_vids.push_back(myvid);

    cout << "[Neighbor Update] Size of list is " << p_sids.size() << endl;
    if (unknown_ids) {
        createIntervalFiles(&p_vids, &p_sids, myvid);
    } else {
        openIntervalFiles(&p_vids, myvid);
    }

    print_all_db_ranges();

    ringState rs;
    manager->get_ring_state(rs);
    for (uint i = 0; i < rs.nodes.size(); i++) {
        NodeData n = rs.nodes[i];
        if (getID(myIP, myPort) != getID(n.ip, n.port)) {
            assert(connect_to_backend("", "", getID(n.ip, n.port), false) != NULL);
        }
    }
    ring = new Ring();
    ring->UpdateState(rs);
}

void node_mgr::heartbeat_response(const std::string& ip) {
    // Your implementation goes here
    printf("heartbeat_response\n");
}

void node_mgr::print_neighbours() {
    for (uint i = 0; i < m_VnodeIDs.size(); i++) {
        print_neighbours(m_VnodeIDs[i]);
    }
}

void node_mgr::print_neighbours(const std::string& vid) {
    pair <string, string> p;
    p = vid_pid_predIP_map[vid];
    cout << bytes_to_hex(vid) << " :: pred<" << bytes_to_hex(p.first) << " , " << p.second << ">";

    p = vid_sid_succIP_map[vid];
    cout << ";  succ<" << bytes_to_hex(p.first) << " , " << p.second << ">" << endl;
}
