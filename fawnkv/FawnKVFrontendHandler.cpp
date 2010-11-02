/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "ring.h"
#include "FawnKVFrontend.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using fawn::DBID;
using namespace fawn;
#define String(x) bytes_to_hex(x)

void Ring::chain_repair_done(const std::string& key, const std::string& endkey)
{
    cout << "chain_repair_done! " << endl;
    cout << "[CRD] start key = " << String(key) << endl;
    cout << "[CRD] end key = " << String(endkey) << endl;
    std::list<VirtualNode*>::iterator it;
    VirtualNode* head, *cur, *pred;
    DBID* f, j = DBID(endkey);
    // TODO : lock!
    for (it = nodeList.begin(); it!= nodeList.end(); it++)
    {
        f = ((*it)->id);
        if ( *f == j)
        {
            head = (*it);
            cur = (*it);
            break;
        }
    }
    for (int i = 0; i < replica_group_size; i++)
    {
        if (cur->n->valid) {
          pred = cur;
        }
        cur = getSuccessor(cur->id, true);
    }


    cout << "[CRD] sending vnode_extend_chain to ";
    cur->n->print();
    cur->n->backend->vnode_extend_chain(cur->id->actual_data_str(),
                                        pred->id->actual_data_str(),
                                        key,
                                        endkey,
                                        pred->n->IP,
                                        pred->n->port, PLAIN);

}

void Ring::static_join(const std::string& ip, const int32_t port) {
    Node *n = createNode(ip, port);

    //cout << "Sending back a static join response." << endl;

    u_int32_t ring_id = fawn::HashUtil::BobHash(getID(ip, port));

    pthread_mutex_lock(&phy_nodelist_mutex);
    phyNodeList.push_front(n);
    // Increment # of nodes that have joined
    numJoinedPhyNodes++;
    pthread_mutex_unlock(&phy_nodelist_mutex);

    vector<string> vids;
    for (int i = 0; i < NUM_VIRTUAL_IDS; i++) {
        DBID id((char *)&ring_id, sizeof(u_int32_t));
        staticJoin(id, n);

        string s(id.data(), id.size());
        vids.push_back(s);
        ring_id = fawn::HashUtil::hashint_full_avalanche_1((u_int32_t)ring_id);
    }

    joinInProgress = true;
    n->backend->static_join_response(vids, myIP);
    printRing();

}

void Ring::rejoin(const std::vector<std::string> & vnodeids, const std::vector<std::string> & startids, const std::string& ip, const int32_t port) {
    Node *n = createNode(ip, port);
    joinInProgress = true;

    if (vnodeids.size() == 0) {
        // Don't know your ids, so just join and wait for neighbor update to inform neighbors/ids.
        joinRing(ip, port, n);
        //sendJoinRsp(n);
    } else {
        pthread_mutex_lock(&phy_nodelist_mutex);

        phyNodeList.push_front(n);
        // Increment # of nodes that have joined
        numJoinedPhyNodes++;

        pthread_mutex_unlock(&phy_nodelist_mutex);

        for (int i = 0; i < vnodeids.size(); i++) {
            string vnodeid = vnodeids[i];
            DBID id(vnodeid);
            staticJoin(id, n);
        }
        //sendReJoinRsp(n);
        // just send a dummy response for now
        // will send neighbor IPs in a separate message later.
        n->backend->rejoin_response(vnodeids, myIP);
    }

}

// This calls init response, not join_response
// join would just create the intervals, which is not useful
void Ring::join(const std::string& ip, const int32_t port, bool merge) {
    Node *n = createNode(ip, port);

    //cout << "Sending back an init response." << endl;
    u_int32_t ring_id = fawn::HashUtil::BobHash(getID(ip, port));

    vector<string> vnodeids;
    for (int i = 0; i < NUM_VIRTUAL_IDS; i++) {
        DBID id((char *)&ring_id, sizeof(u_int32_t));

#ifdef DEBUG
        cout << "virtual node " << i << endl;
        //print_payload(ring_id, 20);
        id.printValue();
        cout << endl;
#endif
        string s(id.data(), id.size());
        vnodeids.push_back(s);
        ring_id = fawn::HashUtil::hashint_full_avalanche_1((u_int32_t)ring_id);
    }

    joinInProgress = true;
    n->backend->init_response(vnodeids, myIP);

    // Join response, not needed.
    // vector<string> startids, vnodeids;
    // list<DBID>::const_iterator i;
    // for (i = n->vnodes.begin(); i != n->vnodes.end(); i++) {
    //     string s((*i).data(), (*i).size());
    //     vnodeids.push_back(s);
    //     DBID* p_pred = getPredecessorID(*i, true);
    //     string sp;

    //     if (p_pred != NULL) {
    //         sp.append(p_pred->data(), p_pred->size());
    //     } else {
    //         sp.append(s);
    //     }
    //     startids.push_back(sp);
    // }

    // n->backend->join_response(vnodeids, startids, myIP);



}

void Ring::vnode_pre_join(const std::string& vid, const std::string& ip, const int32_t port) {
    Node *n = createNode(ip, port); // already created

    //cout << "Sending back a pre-join response." << endl;

    // For this vnode, find the ranges that it is responsible for
    // and create a vnode-pre-join-response with the following information
    // For each range R that the vnode is a replica for
    // send <start_id, end_id, tail_ip>

    // TODO: Lock Ring State and Unlock only when node has fully joined.
    // Ideally lock key-ranges so that other vnodes can join if not in the same key-range.

    DBID db_vid(vid);
    vector<string> sids;
    vector<string> eids;
    vector<string> tail_ips;
    vector<int32_t> tail_ports;

    vector<string>* p_vnbrs = getNbrhood(&db_vid);
    if (p_vnbrs->size() == (replica_group_size * 2)) {
        for (u_int i = 0; i < replica_group_size; i++) {
            sids.push_back((*p_vnbrs)[i]);
            if ( i != (replica_group_size - 1) ) {
                eids.push_back((*p_vnbrs)[i + 1]);
            } else {
                eids.push_back(vid);
            }
            tail_ips.push_back(getIP((*p_vnbrs)[i + replica_group_size]));
            tail_ports.push_back(getPort((*p_vnbrs)[i + replica_group_size]));
        }
    } else if (p_vnbrs->size() == 3) { // for R = 1
        sids.push_back((*p_vnbrs)[0]);
        eids.push_back((*p_vnbrs)[1]);
        tail_ips.push_back(getIP((*p_vnbrs)[2]));
        tail_ports.push_back(getPort((*p_vnbrs)[2]));
    } else if (p_vnbrs->size() == 1) { // First node joining
        sids.push_back((*p_vnbrs)[0]);
        eids.push_back((*p_vnbrs)[0]);
    } else {
        cout << "error in nbrhood" << endl;
    }
    delete p_vnbrs;

    n->backend->vnode_pre_join_response(vid, sids, eids, tail_ips, tail_ports, myIP);
}

// now takes a merge flag determing whether its MERGE, SPLIT, or PLAIN
/**
 * vid = joining node's id, except for the following cases
 * when a node fails, we extend chains, and vid is the startID of the range in question:
 *  a) for the case where the failing node was the head of a chain, and we have extended the chain to incorporate a new tail node: the merge flag is set to MERGE
 *  b) for the other chains that the failing node was a part of, n, and we have extended the chain to incorporate a new tail node: the merge flag is set to PLAIN
 *
**/
void Ring::vnode_join(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge) {

  if ((merge == MERGE) || (merge == PLAIN)) {
    vnode_join_extend_chain(vid, ip, port, merge);
    return;
  }

    DBID db_vid(vid);
    Node* joiner = createNode(ip, port); // will get existing node
    VirtualNode* joiner_vn = new VirtualNode(joiner, &db_vid);
    string joiner_ip = ip;

    deque<VirtualNode*> temp_vnbrs;

    pthread_mutex_lock(&virtual_nodelist_mutex);

    cout << "numJoinedPhyNodes: " << numJoinedPhyNodes << endl;
    // Need a cleaner way to add nodes 1...num_replicas.
    if (numJoinedPhyNodes == 0) {
        // No nodes joined yet just need to send CMM with hops = 1
        vector<string> neighbor_ips, forwarding_ips, neighbor_vids;
        vector<int32_t> neighbor_ports, forwarding_ports;
        neighbor_vids.push_back(vid);
        neighbor_ips.push_back(joiner_ip);
        forwarding_ips.push_back(joiner_ip);
        neighbor_ports.push_back(port);
        forwarding_ports.push_back(port);

        // TODO A) expect 1 CMM to come back before considering the vnode as joined
        joiner->backend->flush_split(vid, vid, vid, joiner_ip, forwarding_ips, neighbor_ips, neighbor_vids, 1, forwarding_ports, neighbor_ports, myIP);
    } else {
        /*
          CMM contains information only for one range R = [startID, endID]
          CMM fields: startID, endID, VID, and IPs of chain, and hops
          IPs are [joinIP, headIP, midIPs..., tailIP, oldTailIP, joinIP] for range R.
          hops = replica_group_size+2
        */

        DBID *temp_id = &db_vid;

        for (u_int i = 0; i < replica_group_size; i++) {
            VirtualNode *temp_vn = getPredecessor(temp_id, false);
            if (temp_vn == NULL)
                break;
            temp_id = temp_vn->id;
            temp_vnbrs.push_front(temp_vn);
        }

        temp_vnbrs.push_back(joiner_vn);
        temp_id = &db_vid;
        for (u_int i = 0; i < replica_group_size; i++) {
            VirtualNode *temp_vn = getSuccessor(temp_id, false);
            if (temp_vn == NULL)
                break;
            temp_id = temp_vn->id;
            temp_vnbrs.push_back(temp_vn);
        }


        for (u_int i = 0; i < replica_group_size; i++) {
            vector<string> neighbor_ips, forwarding_ips, neighbor_vids;
            vector<int32_t> neighbor_ports, forwarding_ports;

            string sid(temp_vnbrs[i]->id->data(), temp_vnbrs[i]->id->size());
            string eid(temp_vnbrs[i+1]->id->data(), temp_vnbrs[i+1]->id->size());


            // this is done so that the joiner sets-up a db for new updates
            forwarding_ips.push_back(joiner_ip);
            forwarding_ports.push_back(port);
            u_int num_replicas_added = 0;
            bool seen_self = false;
            for (u_int j = 1; num_replicas_added < replica_group_size; j++) {
                if (getID(joiner_ip, port) != getID(temp_vnbrs[i+j]->n->IP, temp_vnbrs[i+j]->n->port)) {
                    forwarding_ips.push_back(temp_vnbrs[i+j]->n->IP);
                    forwarding_ports.push_back(temp_vnbrs[i+j]->n->port);
                    num_replicas_added++;
                } else {
                    if (!seen_self)
                        seen_self = true;
                    else {
                        cerr << "joining node has virtual nodes that are close " <<
                             "to each other on the ring. (not good for replication)" << endl;
                        exit(1);
                    }
                }
            }
            forwarding_ips.push_back(joiner_ip);
            forwarding_ports.push_back(port);

            for (u_int j = 1; j <= replica_group_size+1; j++) {
                neighbor_ips.push_back(temp_vnbrs[i+j]->n->IP);
                neighbor_ports.push_back(temp_vnbrs[i+j]->n->port);
                neighbor_vids.push_back(string(temp_vnbrs[i+j]->id->data(), temp_vnbrs[i+j]->id->size()));
            }

            // TODO A) expect replica_group_size CMMs to come back (for this vnode) before considering the vnode as joined
            joiner->backend->flush_split(sid, eid, vid, joiner_ip, forwarding_ips, neighbor_ips, neighbor_vids, replica_group_size+2, forwarding_ports, neighbor_ports, myIP);
        }

        // A new node joining as a tail (to range ending with temp_vnbrs[1]) cannot
        // respond to gets until CMM is received.
        // predecessor must serve it in the interim.
        string tempTailVid(temp_vnbrs[1]->id->data(), temp_vnbrs[1]->id->size());
        vid_temptail_map[tempTailVid] = true;
    }

    //printRing();
    cout << endl;
    // Add vnode to ring state.
    pthread_mutex_lock(&phy_nodelist_mutex);
    phyNodeList.push_front(joiner);
    // Increment # of nodes that have joined
    numJoinedPhyNodes++;
    pthread_mutex_unlock(&phy_nodelist_mutex);
    staticJoin(db_vid, joiner, false);
    cout << "Ring: " << endl;
    printRing();

    pthread_mutex_unlock(&virtual_nodelist_mutex);

}

void Ring::vnode_join_extend_chain(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge) {

  DBID foo = DBID(vid);
  string succ = getSuccessor(&foo, true)->id->actual_data_str();
  string succ_succ = getSuccessor(getSuccessor(&foo, true)->id, true)->id->actual_data_str();
  NodeHandle* group = getReplicaGroup(&DBID(succ));

  ///
  #define toString(x) getID(x->IP, x->port)
  cout << "[vnode join] Printing NodeHandle" << endl;
  cout << "\t\tOwner is " << toString(group->getOwner()) << endl;
  for (int i=0; i< group->replicaGroup.size();i++)
    cout << "\t\tReplica " << i << " is " << toString(group->replicaGroup[i]) << endl;
  cout << "--------- End NodeHandle -----" << endl;
  ///

  vid_shorter_map[vid] = false; // safe to always set this to false regardless of range.
  if (merge == MERGE)
    {

      cout << "[vnode_join] MERGE" << endl;
      cout << "[VJ] Sending to " << getID(group->replicaGroup[0]->IP, group->replicaGroup[0]->port) << endl;
      cout << "[VJ] With args : \n" << endl;
      cout << "[VJ] \t\t" << "start = " << bytes_to_hex(vid) << endl;
      cout << "[VJ] \t\t" << "end = " << bytes_to_hex(succ) << endl;
      cout << "[VJ] \t\t" << "m_start = " << bytes_to_hex(succ) << endl;
      cout << "[VJ] \t\t" << "m_end = " << bytes_to_hex(succ_succ) << endl;

      cout << "[VJ] \t\t" << "hopc = " << replica_group_size-1 << endl;

      group->replicaGroup[0]->backend->flush_merge(vid,
                                                   succ,
                                                   succ,
                                                   succ_succ,
                                                   replica_group_size-1);
    }

    else if (merge == PLAIN)
    {
      cout << "[vnode_join] PLAIN" << endl;

      group->getOwner()->backend->flush(vid, succ, replica_group_size-1);
    }
}

void Ring::flush_nosplit(const std::string& vid)
{
    cout << "[FNS] Node join complete for range " << bytes_to_hex(vid) << endl;
    vid_temptail_map[vid] = false; // safe to always set this to false regardless of range.
    vid_shorter_map[vid] = false; // safe to always set this to false regardless of range.
    hasNodes = true;
    joinInProgress = false;
    // TODO A) remove node from Ring (when we get flush for all chains that the node was a part of)
}

void Ring::flush_split(const std::string& start_id, const std::string& end_id, const std::string& vid, const std::string& joiner_ip, const std::vector<std::string> & forwarding_ips, const std::vector<std::string> & neighbor_ips, const std::vector<std::string> & neighbor_vids, const int32_t hops, const std::string& ip) {
    cout << "Node join complete!" << endl;

    assert(hops == 0);
    vid_temptail_map[vid] = false; // safe to always set this to false regardless of range.
    hasNodes = true;
    joinInProgress = false;
}

void Ring::put_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip) {
    DPRINTF(DEBUG_FLOW, "\t Key: %s\n", key.c_str());
    DPRINTF(DEBUG_FLOW, "\t Continuation: %d\n", continuation);

    if (NULL != put_cb) {
        put_cb(continuation);
    }

}

void Ring::get_response(const std::string& key, const std::string& value, const int64_t continuation, const bool success, const std::string& ip) {
#ifdef DEBUG
    // Print data
    cout << "\t Key: " << key << endl;
    cout << "\t Value: '" << value << "'" <<  endl;
    cout << "\t Continuation: " << continuation << endl;
    cout << "\t Success: " << success << endl;
#endif
    if (NULL != get_cb) {
        DBID dkey(key);
        get_cb(dkey, value, continuation, success);
    }

}

void Ring::remove_response(const std::string& key, const int64_t continuation, const bool success, const std::string& ip) {
    // Your implementation goes here
    printf("remove_response\n");
}

void Ring::heartbeat(const std::string& ip) {
    if ( ip_node_map.find(ip) != ip_node_map.end())
    {
        if (ip_node_map[ip]->get_beats_missed() > 2)
        {
            printf("heartbeat from %s\n", ip.c_str());
        }
        ip_node_map[ip]->setAlive();
    } else {
        printf("%s is not in our ip_node map... \n", ip.c_str());
    }
}
