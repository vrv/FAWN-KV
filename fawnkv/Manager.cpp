/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <iostream>
#include <list>
#include <map>
#include <sys/types.h>
#include <arpa/inet.h>
#include <iomanip>
#include <pthread.h>
#include <sched.h>
#include <netinet/tcp.h>
#include "print.h"
#include "Manager.h"

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TSocket.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace boost;
using namespace std;

using fawn::DBID;

#define String(x) bytes_to_hex(x)

void Manager::verifyThrift(VirtualNode* failed)
{
    /*
      VirtualNode* cur = getSuccessor(failed->id, true);
      cout << "--------------Testing Ring ----------" << endl;
      cout << "failing node's port is " << failed->n->port << endl;
      cur->n->backend->integrity(3);
      //this only works on uniqued port systems
      while(cur->n->port != failed->n->port)
      {
      cout << "[Verify Thrift] Testing " << getID(cur->n->IP, cur->n->port) << endl;
      cur->n->backend->onetest(string("foo"), string("bar"), string("baz"));
      cout << "twotest returns : " <<  cur->n->backend->twotest(string("apples"), string("bananas"), string("oranges")) << endl;
      cur = getSuccessor(cur->id, true);

      }
    */
}

/** not sure where its new home should be, but had to salvage this
 * function from deletion
 */
void *agingThreadLoop(void *p)
{
    Manager* r = (Manager*)p;

    while (1) {
        sleep(AGING_INTERVAL_IN_SEC);
        r->ageNodes();
    }

    return NULL;
}



Manager::Manager(string localIP, int num_replicas) {
    init(localIP, num_replicas);
}

void Manager::init(string localIP, int num_replicas) {
    myIP.assign(localIP);
    replica_group_size = num_replicas;

    numJoinedPhyNodes = 0;

    pthread_mutex_init(&phy_nodelist_mutex, NULL);
    pthread_mutex_init(&virtual_nodelist_mutex, NULL);

    // /* Start node aging thread */
    pthread_t agingThreadId_;
    pthread_create(&agingThreadId_, NULL,
                   agingThreadLoop, (void*)this);

}

Manager::~Manager() {
    /* Cleanup pthreads */
    pthread_mutex_destroy(&phy_nodelist_mutex);
    pthread_mutex_destroy(&virtual_nodelist_mutex);

    list<Node*>::iterator i;
    for (i = phyNodeList.begin(); i != phyNodeList.end(); i++) {
        delete *i;
    }
}


VirtualNode* Manager::getPredecessor(DBID* vn, bool b_lock)
{
    if (b_lock)
        pthread_mutex_lock(&virtual_nodelist_mutex);
    VirtualNode* pred = NULL;
    list<VirtualNode*>::const_iterator i;

    bool b_getpred_done = false;
    // nodelist in ascending order by ID
    for (i = nodeList.begin(); (i != nodeList.end()) && (!b_getpred_done); i++) {
        if ( *((*i)->id) < *vn) {
            pred = *i;
        }
        //else if ( *((*i)->id) == *(vn->id)) {
        else {
            if (i == nodeList.begin()) {
                pred = nodeList.back();
            }

            b_getpred_done = true;
        }
    }

    if (b_lock)
        pthread_mutex_unlock(&virtual_nodelist_mutex);
    return pred;
}


DBID* Manager::getPredecessorID(DBID* vn, bool b_lock)
{
    VirtualNode* vnode = getPredecessor(vn, b_lock);
    if (vnode == NULL)
        return NULL;
    return vnode->id;
}


VirtualNode* Manager::getSuccessor(DBID* vn, bool b_lock)
{
    if (b_lock)
        pthread_mutex_lock(&virtual_nodelist_mutex);

    VirtualNode* p_succ = NULL;

    list<VirtualNode*>::const_iterator i, j;

    // nodelist in ascending order by ID
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        j = i;
        j++;

        if (j == nodeList.end()) {
            j = nodeList.begin();
        }

        if ( *((*i)->id) == *vn ) {
            p_succ = *j;
            break;
        }

    }

    if (p_succ == NULL) {
        for (i = nodeList.begin(); i != nodeList.end(); i++) {
            j = i;
            j++;
            if (j == nodeList.end()) {
                j = nodeList.begin();
            }

            // If vn is not in the nodeList, we need to check ranges.
            if (between((*i)->id, (*j)->id, vn)) {
                p_succ = *j;
                break;
            }
        }
    }


    if (b_lock)
        pthread_mutex_unlock(&virtual_nodelist_mutex);

    return p_succ;
}


DBID* Manager::getSuccessorID(DBID* vn, bool b_lock)
{
    return getSuccessor(vn, b_lock)->id;
}


vector<string>* Manager::getNbrhood(DBID* db_id)
{
    //cout << "In getNbrhood" << endl;
    pthread_mutex_lock(&virtual_nodelist_mutex);
    list<string> temp_vnbrs_list;

    VirtualNode* temp_vn = NULL;
    DBID* temp_id = db_id;

    // get replica_group_size preds
    for (unsigned int i = 0; i < replica_group_size; i++) {
        temp_id = getPredecessorID(temp_id, false);
        if (temp_id == NULL)
            break; // No predecessor
        temp_vnbrs_list.push_front(*(temp_id->actual_data()));
        //delete temp_id->actual_data; Memory leak?
    }

    temp_id = db_id;

    if (replica_group_size == 1) { // For R = 1
        temp_vnbrs_list.push_back(*(db_id->actual_data()));
    }

    // get replica_group_size successors IPs
    for (unsigned int i = 0; i < replica_group_size; i++) {
        temp_vn = getSuccessor(temp_id, false);
        if (temp_vn == NULL)
            break;
        temp_id = temp_vn->id;
        temp_vnbrs_list.push_back(getID(temp_vn->n->IP, temp_vn->n->port));
    }

    vector<string>* p_vnbrs = new vector<string>(temp_vnbrs_list.begin(), temp_vnbrs_list.end());

    pthread_mutex_unlock(&virtual_nodelist_mutex);
    return p_vnbrs;
}


VirtualNode* Manager::getDistinctPredecessor(DBID* vn)
{
    pthread_mutex_lock(&virtual_nodelist_mutex);
    //string* predIP = NULL;
    VirtualNode* p_pred = NULL;

    list<VirtualNode*>::const_iterator i, j;

    bool b_getpred_done = false;
    // nodelist in ascending order by ID
    for (i = nodeList.begin(); (i != nodeList.end()) && (!b_getpred_done); i++) {
        if ( *((*i)->id) == *vn ) {
            const string currIP = (*i)->n->IP;

            j = i;
            if (j == nodeList.begin()) {
                j = nodeList.end();
            }
            j--;

            for (; (j != i) && (!b_getpred_done); j--) {
                if ((*j)->n->IP != currIP) {
                    // done
                    //predIP = new string((*j)->n->IP);
                    p_pred = *j;
                    b_getpred_done = true;
                }
                else if (j == nodeList.begin()) {
                    j = nodeList.end();
                }
            }
        }
    }
    pthread_mutex_unlock(&virtual_nodelist_mutex);
    //return predIP;
    return p_pred;
}


VirtualNode* Manager::getDistinctSuccessor(DBID* vn)
{
    pthread_mutex_lock(&virtual_nodelist_mutex);
    //string* succIP = NULL;
    VirtualNode* p_succ = NULL;

    list<VirtualNode*>::const_iterator i, j;

    bool b_getsucc_done = false;
    // nodelist in ascending order by ID
    for (i = nodeList.begin(); (i != nodeList.end()) && (!b_getsucc_done); i++) {
        if ( *((*i)->id) == *vn ) {
            const string currIP = (*i)->n->IP;

            j = i;
            j++;

            if (j == nodeList.end()) {
                j = nodeList.begin();
            }

            for (; (j != i) && (!b_getsucc_done); j++) {
                if ((*j)->n->IP != currIP) {
                    // done
                    //succIP = new string((*j)->n->IP);
                    p_succ = *j;
                    b_getsucc_done = true;
                }
                else if (j == nodeList.end()) {
                    j = nodeList.begin();
                    // another check
                    if ((*j)->n->IP != currIP) {
                        // done
                        //succIP = new string((*j)->n->IP);
                        p_succ = *j;
                        b_getsucc_done = true;
                    }
                }
            }
        }
    }


    pthread_mutex_unlock(&virtual_nodelist_mutex);
    //return succIP;
    return p_succ;
}


// call after obtaining lock on nodelist
void Manager::print_membership_info()
{
    cout << "membership info (begin)" << endl;
    list<VirtualNode*>::const_iterator i;
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        (*i)->id->printValue();
        cout << "ip: " << (*i)->n->IP << endl;
    }
    cout << "membership info (end)" << endl;
}

#define CR



void Manager::failNode(Node* i)
{
#ifndef CR
    return;
#endif
    std::list<DBID>::iterator v;
    int r = replica_group_size;
    VirtualNode* fail = (VirtualNode*) malloc(sizeof(VirtualNode));
    VirtualNode* cur;
    fail->n = i;
    cout << "Failing node is: ";
    i->print();
    for( v = i->vnodes.begin(); v != i->vnodes.end(); v++) {

        VirtualNode *pred, *succ;
        fail->id = &(*v);
        cur = fail;
        pred = getPredecessor(&(*v), true);
        succ = getSuccessor(&(*v), true);
        cout << "Successor = " << getID(succ->n->IP, succ->n->port) << endl;

        cout << "Handling failure of vnode: " <<  bytes_to_hex(v->actual_data_str()) << endl;
        //update our virtualnode to have the correct id

        if (r == 1) {

            succ->n->backend->chain_repair_single(pred->id->actual_data_str(), succ->id->actual_data_str());

            cout << "removing node from ring" << endl;
            removeNodefromRing(i, true);
            removePhysicalNode(i);
            free(fail);
            return;

        } else {

            /* TAIL CASE */

            /* lets grab the head of the range for which v is
             * the tail
             *
             * tail to head is R-1 steps
             */
            for(int j=0; j < r-1; j++) {
                cur = getPredecessor(cur->id, true);
            }
            cout << "[VID_SHORTER_MAP] Head of range for which we were the tail is : " << bytes_to_hex(cur->id->actual_data_str()) << endl;
            vid_shorter_map[cur->id->actual_data_str()] = true;
            cout << "Sending CR_T with args: " << bytes_to_hex(cur->id->actual_data_str()) << " , " << r-2 << " , " << r-2 << endl;
            cur->n->backend->chain_repair_tail(cur->id->actual_data_str(),
                                               r-2,
                                               r-2,
                                               succ->id->actual_data_str(),
                                               getID(succ->n->IP, succ->n->port) );


            /** TODO tell sucessor of failed vnode to join this chain .... **/

            /** MID CASE **/


            /**
             * cur is currently the head of the chain for which v is the tail.
             * all vnodes between cur and v represent chains for which v is a mid
             */
            int k = 1;
            string s;
            for( cur = getSuccessor(cur->id, true);
                 !(*(cur->id) == (*v));
                 cur = getSuccessor(cur->id, true)
                 )
                {
                    cout << "[Handling mid case, v is " << bytes_to_hex((*v) .actual_data_str()) << endl;
                    cout << "VID shorter mapping : " << bytes_to_hex(cur->id->actual_data_str()) << endl;
                    vid_shorter_map[cur->id->actual_data_str()] = true;
                    string ipport = getID(succ->n->IP, succ->n->port);
                    cout << "Sending CR_M with args: Key=" << bytes_to_hex(cur->id->actual_data_str()) << " , my_vid=" << bytes_to_hex(pred->id->actual_data_str()) << " , succ_vid=" << bytes_to_hex(pred->id->actual_data_str()) << " , succip=" << ipport << " , hopc=" << r-2-k << " , ahopc=" << k ;
                    pred->n->backend->chain_repair_mid(cur->id->actual_data_str(),
                                                       pred->id->actual_data_str(),
                                                       succ->id->actual_data_str(),
                                                       ipport,
                                                       r-2-k,
                                                       k);
                    k++;
                }

            /* HEAD CASE
             * 1. remove the vnode from the ring structure
             * 2. shorten the (merging) range's R
             */

            //        first, need to find the node we're going to have join this chain

            cur = fail;
            cout << "figuring out extend_chain magic" << endl;
            cout << "Failed is " << getID(cur->n->IP, cur->n->port) << endl;
            cout << "We want to be merging " << bytes_to_hex(cur->id->actual_data_str()) << " and " << bytes_to_hex(getPredecessor(cur->id, true)->id->actual_data_str()) << endl;

            for(int j = 0; j < r; j++) {
                pred = cur;
                cur = getSuccessor(cur->id, true);
                cout << "Cur is " << getID(cur->n->IP, cur->n->port) << endl;
            }

            cout << "Sending extend chain to " << getID(cur->n->IP, cur->n->port) << " who will hopefully req from " << getID( pred->n->IP, pred->n->port) << "for range " << "[" << bytes_to_hex(getPredecessor(fail->id, true)->id->actual_data_str()) << ", " << bytes_to_hex(fail->id->actual_data_str()) << "]" << endl;

            cur->n->backend->vnode_extend_chain(cur->id->actual_data_str(),
                                                pred->id->actual_data_str(),
                                                getPredecessor(fail->id, true)->id->actual_data_str(),
                                                fail->id->actual_data_str(),
                                                pred->n->IP,
                                                pred->n->port,
                                                MERGE);

            cout << "shortening range: " << getSuccessorID(fail->id, true)->actual_data_str() << endl;

            vid_shorter_map[getSuccessorID(fail->id, true)->actual_data_str()] = true;
        }
    }
    free(fail);
    i->valid = false;
}


//#define NOFAIL
void Manager::ageNodes() {
#ifdef NOFAIL
    return;
#endif
    pthread_mutex_lock(&phy_nodelist_mutex);

    bool b_empty = false;
    if (phyNodeList.empty()) {
        b_empty = true;
    }

    if (!b_empty) {
        list<Node*>::iterator i;


        for (i = phyNodeList.begin(); i != phyNodeList.end();) {
            (*i)->increment_beats_missed();
#ifdef DEBUG
            cout << "(AGE) beats missed [" << (*i)->IP << "]: " << (*i)->get_beats_missed() << endl;
#endif
            if((*i)->valid == false)
                {
                    i++;
                    //                cout << "Skipping a dead node..." << endl;

                    continue;
                }


            if ((*i)->get_beats_missed() >= NUM_BEATS_BEFORE_FAILURE) {
                cout << "Failure: " << (*i)->IP << endl;


                /** For the time being, we're assuming a one to one
                 * mapping from vnodes to phynodes, as without better
                 * distribution strategies physical node failures
                 * could get hairy quickly
                 **/

                cout << "Failing a node from ageNodes" << endl;
                string s;
                //                cin >> s;
                failNode(*i);

                //                delete(*i); //also don't think we need this?
                //                i = phyNodeList.erase((i));
            } else {
                i++;
            }

        }
    }

    pthread_mutex_unlock(&phy_nodelist_mutex);
}

/*
 * jhferris 3/21/10
 *
 * basically the first part of getReplicaGroup, we need to be able to
 * identify the range k is from so that we can handle a shortened
 * chain length.
 */

DBID* Manager::getRangeKey(DBID *k) {

#ifdef DEBUG
    cout << "In getRangeKey" << endl;
    cout << "Looking for key: " << endl;
    k->printValue();
#endif

    pthread_mutex_lock(&virtual_nodelist_mutex);
    list<VirtualNode*>::const_iterator i;

    if (!nodeList.empty()) {
        for (i = nodeList.begin(); i != nodeList.end(); i++) {
            DBID* p_id = (*i)->id;
            if ( (*k < *p_id) || (*k == *p_id) ) {
                pthread_mutex_unlock(&virtual_nodelist_mutex);
                return p_id;
            }
        }
#ifdef DEBUG
        cout << "Owner is first node" << endl;
#endif
        /* If we didn't find owner in previous loop */
        /* then the owner is the first node*/
        if (i == nodeList.end()) {
            i = nodeList.begin();
            pthread_mutex_unlock(&virtual_nodelist_mutex);
            return (*i)->id;
        }
    }
    return NULL;
}


NodeHandle* Manager::getReplicaGroup(DBID *k) {
#ifdef DEBUG
    cout << "[GRG] Looking for key: " <<  bytes_to_hex(k->actual_data_str()) << endl;
#endif

    pthread_mutex_lock(&virtual_nodelist_mutex);

    NodeHandle *nh = NULL;

    if (!nodeList.empty()) {
        std::list<Node*> replicas;
        list<VirtualNode*>::const_iterator i;
        list<Node*>::iterator r_iter;
        nh = new NodeHandle();
        Node *owner = NULL;
        unsigned int num_replicas = 0;

        for (i = nodeList.begin(); i != nodeList.end(); i++) {
            DBID* p_id = (*i)->id;
            if ( (*k < *p_id) || (*k == *p_id) ) {
                replicas.push_back((*i)->n);
                //                (*i)->n->print();
                owner = (*i)->n;
                num_replicas++;
#ifdef DEBUG
                cout << "[GRG] Owner VID is " << bytes_to_hex((*i)->id->actual_data_str()) << endl;
                cout << "Replica handling this request: " << (*i)->n->IP << endl;
#endif
                //(*i)->id->printValue();
                break;
            }
        }
#ifdef DEBUG
        cout << "Owner is first node" << endl;
#endif
        /* If we didn't find owner in previous loop */
        /* then the owner is the first node*/
        if (i == nodeList.end() && num_replicas == 0) {
            i = nodeList.begin();
            replicas.push_back((*i)->n);

            //            (*i)->n->print();
            owner = (*i)->n;
            num_replicas++;

#ifdef DEBUG
            cout << "[GRG] Owner VID is " << bytes_to_hex((*i)->id->actual_data_str()) << endl;
            cout << "Replica handling this request: " << (*i)->n->IP << ":" << owner->port <<
                " (" << bytes_to_hex((*i)->id->actual_data_str()) << ")" << endl;
#endif
        }

        nh->setOwner(owner);
#ifdef DEBUG
        cout << "\t owner - " << owner->IP << ":" << owner->port << endl;
#endif


        // Invariant: i points to owner and replicas includes owner at head

        int uniqueReplica = 1;

        // Increment to move past owner
        i++;
        // Wrap around
        if (i == nodeList.end()) {
            i = nodeList.begin();
        }

        int num_items = nodeList.size();
        int num_examined = 1;
        // Clockwise search from 0 through ring to find replicas
        while ( ((num_items - num_examined) > 0) && (num_replicas < replica_group_size)) {

            if (!((*i)->n == owner)) {
#ifdef DEBUG
                cout << "\t examining - " << (*i)->n->IP << ":" << (*i)->n->port << " --> vnode id: " <<
                    bytes_to_hex((*i)->id->actual_data_str()) << endl;
#endif

                uniqueReplica = 1;
                for (r_iter = replicas.begin(); r_iter != replicas.end(); r_iter++) {
                    if (( *((*i)->n) == *(*r_iter) ) ) {
                        uniqueReplica = 0;
                    }
                }

                // Node is not already in replics, add it
                if (uniqueReplica) {
                    replicas.push_back((*i)->n);
                    //                    (*i)->n->print();
                    num_replicas++;
                }
            }

            i++;

            // Wrap around
            if (i == nodeList.end()) {
                i = nodeList.begin();
            }
            num_examined++;

        }

        // Get replica iterator
        r_iter = replicas.begin();

        // Skip past owner (already added above)
        r_iter++;
        DPRINTF(DEBUG_FLOW, "Adding replicas to node handle\n");

        // Add replicas to node handle
        while (r_iter != replicas.end()) {
            nh->addReplica((*r_iter));
            r_iter++;
        }
        DPRINTF(DEBUG_FLOW, "Found %d replicas\n", num_replicas);
    }

    pthread_mutex_unlock(&virtual_nodelist_mutex);
    return nh;
}


void Manager::addNodetoRing(VirtualNode* n, bool lock) {

    if (lock)
        pthread_mutex_lock(&virtual_nodelist_mutex);

    bool b_empty = false;
    if (nodeList.empty()) {
        nodeList.push_front(n);
        b_empty = true;
    }

    if (!b_empty) {
        list<VirtualNode*>::iterator i;

        bool add_done = false;
        /* Add node to list in ascending order by ID*/
        for (i = nodeList.begin(); (i != nodeList.end()) && !add_done; i++) {

            if (*(n->id) < *((*i)->id)) {
                nodeList.insert(i, n);
                add_done = true;
            } else if ( *((*i)->id) == *(n->id)) {
                cerr << "Manager::addNodetoRing failed, ID already exists"<<endl;
                string vid = bytes_to_hex((const u_char*)n->id->data(), n->id->size());
                string vid2 = bytes_to_hex((const u_char*)(*i)->id->data(), (*i)->id->size());
                cerr << "failed on " << vid <<  " colided with " << vid2 << endl;
                add_done = true;
            }
        }

        if (!add_done) {
            nodeList.push_back(n);
        }
    }

    if (lock)
        pthread_mutex_unlock(&virtual_nodelist_mutex);

}


/* Removes all virtual nodes for given physical node */
void Manager::removeNodefromRing(Node* n, bool lock) {
    if (lock)
        pthread_mutex_lock(&virtual_nodelist_mutex);

    cout << "Target is: " << n->IP << ":" << n->port << endl;
    if (!nodeList.empty()) {
        list<VirtualNode*>::iterator i;

        for (i = nodeList.begin(); i != nodeList.end(); ) {
            if ( *((*i)->n) == *n) {
                cout << "Found match, removing... " << (*i)->n->IP << ":" << (*i)->n->port << endl;
                delete(*i);
                i = nodeList.erase(i);
            } else {
                i++;
            }
        }
    }
    if (lock)
        pthread_mutex_unlock(&virtual_nodelist_mutex);
}


/* Removes a virtual node */
void Manager::removeVirtualNodefromRing(DBID* id, bool lock) {
    if (lock)
        pthread_mutex_lock(&virtual_nodelist_mutex);

    cout << "Target is: ";
    cout <<  bytes_to_hex(id->data());
    cout << "aka \n";
    id->printValue();
    cout << "aka \n";
    printf("%p: 0x%X\n", id->data(), (int) *((int*)id->data()));
    cout << endl;

    if (!nodeList.empty()) {
        list<VirtualNode*>::iterator i;

        for (i = nodeList.begin(); i != nodeList.end(); ) {
            cout << endl;
            if ( *((*i)->id) == *id) {
                cout << "Found match, removing..."<< (*i)->n->IP <<endl;
                delete(*i);
                i = nodeList.erase(i);
                break;
            } else {
                cout << "Does not match:..."<< (*i)->n->IP <<endl;
                i++;
            }
        }
    }
    if (lock)
        pthread_mutex_unlock(&virtual_nodelist_mutex);
}


// no locking here - caller locks nodelist
void Manager::printNodes() {
    if (nodeList.empty()) {
        cout << "->[NULL]\n";
        return;
    }

    list<VirtualNode*>::const_iterator i;

    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        cout << " -> ";
        (*i)->id->printValue();
        cout << "(" << (*i)->n->IP << ")";
    }
    cout << endl;
}


Node* Manager::createNode(string ip, const int32_t port)
{
    // Check if you already have a connection to the node
    string id = getID(ip, port);
    if (ip_node_map.find(id) == ip_node_map.end()) {
        Node *n = new Node(ip, port);
        ip_node_map[id] = n;
        return n;
    } else {
        return ip_node_map[id];
    }
}

void Manager::staticJoin(const DBID id, Node* n, bool lock) {
    // Add virtual node to ring
#ifdef DEBUG
    id.printValue();
    cout << endl;
#endif
    DBID tid = id;
    VirtualNode* vn = new VirtualNode(n, &tid);
    n->addVnode(tid);
    printf("Static Join: \n");
    cout << "\t" << n->IP << ":" << n->port << " --> vnode id: " <<
        bytes_to_hex(vn->id->actual_data_str()) << endl;
    /*
      printf("\t%p: 0x%X\n", id.value, (int) *((int*)id.value));
      printf("\t%p: 0x%X\n", tid.value, (int) *((int*)tid.value));
      printf("\t%p: 0x%X\n", vn->id->value, (int) *((int*)vn->id->value));
    */
    cout << endl;
    addNodetoRing(vn, lock);
}

void Manager::joinRing(string ip, int32_t port, Node* n) {
    pthread_mutex_lock(&phy_nodelist_mutex);
    phyNodeList.push_front(n);
    // Increment # of nodes that have joined
    numJoinedPhyNodes++;
    pthread_mutex_unlock(&phy_nodelist_mutex);

    /* Add NUM_VIRTUAL_IDS virtual nodes to ring */
    DPRINTF(DEBUG_FLOW, "%s\n", ip.c_str());

    u_int32_t ring_id = fawn::HashUtil::BobHash(getID(ip, port));

    for (int i = 0; i < NUM_VIRTUAL_IDS; i++) {
        DBID id((char *)&ring_id, sizeof(u_int32_t));

#ifdef DEBUG
        cout << "virtual node " << i << endl;
        //print_payload(ring_id, 20);
        id->printValue();
        cout << endl;
#endif
        VirtualNode* vn = new VirtualNode(n, &id);
        n->addVnode(id);
        addNodetoRing(vn);
        ring_id = fawn::HashUtil::hashint_full_avalanche_1((u_int32_t)ring_id);
    }

}

void Manager::sendNeighborUpdates()
{
    cout << "Sending neighbor updates to all nodes." << endl;

    if (nodeList.empty()) {
        DPRINTF(DEBUG_FLOW, "No nodeLists to send neighbor updates to.");
        return;
    }

    deque<DBID*> pred_vids;

    list<VirtualNode*>::const_iterator i;
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        string myvid((*i)->id->data(), (*i)->id->size());
        vector<string> pred_vids_list;
        cout << "For IP: " << getID((*i)->n->IP, (*i)->n->port) << endl;
        VirtualNode* successor_vn = getSuccessor((*i)->id, false);
        string succid = getID(successor_vn->n->IP, successor_vn->n->port);
        string succ_vid(successor_vn->id->data(), successor_vn->id->size());
        cout << "\tSuccessor IP: " << succid << endl;

        VirtualNode* predecessor_vn = getPredecessor((*i)->id, false);
        string predid = getID(predecessor_vn->n->IP, predecessor_vn->n->port);
        string pred_vid(predecessor_vn->id->data(), predecessor_vn->id->size());
        cout << "\tPredecessor IP: " << predid << endl;

        DBID *temp_id = (*i)->id;
        for (u_int j = 0; j < replica_group_size; j++) {
            temp_id = getPredecessorID(temp_id, false);
            pred_vids.push_front(temp_id);
        }

        for (u_int j = 0; j < replica_group_size; j++) {
            DBID *pvid = pred_vids[j];
            string vid(pvid->data(), pvid->size());
            pred_vids_list.push_back(vid);
        }

        //cout << "Sending neighbor information to " << (*i)->n->IP << endl;

        (*i)->n->backend->neighbor_update(myvid, pred_vids_list, succ_vid, succid, pred_vid, predid, myIP);
    }

}


void Manager::printRing() {
    list<VirtualNode*>::const_iterator i;
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        string vid = ((*i)->id)->actual_data_str();
        string vid_hex(bytes_to_hex(vid));

        cout << "IP: " << (*i)->n->IP << ":" << (*i)->n->port << ", ID:" << vid_hex << endl;
    }
}

void Manager::removePhysicalNode(Node *n) {
    pthread_mutex_lock(&phy_nodelist_mutex);
    phyNodeList.remove(n);
    delete n;
    numJoinedPhyNodes--;
    pthread_mutex_unlock(&phy_nodelist_mutex);
}


void Manager::chain_repair_done(const std::string& key, const std::string& endkey)
{
    cout << "chain_repair_done! " << endl;
    cout << "[CRD] start key = " << String(key) << endl;
    cout << "[CRD] end key = " << String(endkey) << endl;
    std::list<VirtualNode*>::iterator it;
    VirtualNode* head, *cur, *pred;
    DBID* f, j = DBID(endkey);
    // TODO : lock!
    for (it = nodeList.begin(); it!= nodeList.end(); it++) {
        f = ((*it)->id);
        if ( *f == j)
            {
                head = (*it);
                cur = (*it);
                break;
            }
    }
    for (uint i = 0; i < replica_group_size; i++) {
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

void Manager::static_join(const std::string& ip, const int32_t port) {
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
    cout << "Sending static join response..." << endl;
    n->backend->static_join_response(vids, myIP);
    printRing();

}

void Manager::rejoin(const std::vector<std::string> & vnodeids, const std::vector<std::string> & startids, const std::string& ip, const int32_t port) {
    Node *n = createNode(ip, port);

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

        for (uint i = 0; i < vnodeids.size(); i++) {
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
void Manager::join(const std::string& ip, const int32_t port, bool merge) {
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

void Manager::vnode_pre_join(const std::string& vid, const std::string& ip, const int32_t port) {
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
void Manager::vnode_join(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge) {

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

void Manager::vnode_join_extend_chain(const std::string& vid, const std::string& ip, const int32_t port, int32_t merge) {

    DBID foo = DBID(vid);
    string succ = getSuccessor(&foo, true)->id->actual_data_str();
    string succ_succ = getSuccessor(getSuccessor(&foo, true)->id, true)->id->actual_data_str();
    DBID succid = DBID(succ);
    NodeHandle* group = getReplicaGroup(&succid);

    ///
#define toString(x) getID(x->IP, x->port)
    cout << "[vnode join] Printing NodeHandle" << endl;
    cout << "\t\tOwner is " << toString(group->getOwner()) << endl;
    for (uint i=0; i< group->replicaGroup.size();i++)
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

void Manager::flush_nosplit(const std::string& vid)
{
    cout << "[FNS] Node join complete for range " << bytes_to_hex(vid) << endl;
    vid_temptail_map[vid] = false; // safe to always set this to false regardless of range.
    vid_shorter_map[vid] = false; // safe to always set this to false regardless of range.
    // TODO A) remove node from Ring (when we get flush for all chains that the node was a part of)
}

void Manager::flush_split(const std::string& start_id, const std::string& end_id, const std::string& vid, const std::string& joiner_ip, const std::vector<std::string> & forwarding_ips, const std::vector<std::string> & neighbor_ips, const std::vector<std::string> & neighbor_vids, const int32_t hops, const std::string& ip) {
    cout << "Node join complete!" << endl;

    assert(hops == 0);
    vid_temptail_map[vid] = false; // safe to always set this to false regardless of range.
}

void Manager::heartbeat(const std::string& ip) {
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

void Manager::get_ring_state(ringState& _return) {
    cout << "getting ring state" << endl;
    // For each node in ring
    //   Create NodeData
    //   Add NodeData to ringState list
    pthread_mutex_lock(&virtual_nodelist_mutex);
    pthread_mutex_unlock(&virtual_nodelist_mutex);
    list<VirtualNode*>::const_iterator i;
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        VirtualNode* vn = *i;
        NodeData nd;
        nd.VnodeID = vn->id->actual_data_str();
        nd.ip = vn->n->IP;
        nd.port = vn->n->port;
        _return.nodes.push_back(nd);
        _return.replication = replica_group_size;
    }

    // Get temptail info
    map<string, bool>::const_iterator t;
    for (t = vid_temptail_map.begin(); t != vid_temptail_map.end(); t++) {
        if ((*t).second) {
            _return.temptails.push_back((*t).first);
        }
    }

    for (t = vid_shorter_map.begin(); t != vid_shorter_map.end(); t++) {
        if ((*t).second) {
            _return.shorters.push_back((*t).first);
        }
    }

}

void usage()
{
    cerr <<  "./manager myIP [-j] \n\n"
         <<   "Options: " << endl
         <<  "-j         use only if backends are static joining.\n"
         << endl;
}

int main(int argc, char **argv)
{
    int ch;
    bool static_join = false;
    while ((ch = getopt(argc, argv, "j")) != -1) {
        switch (ch) {
        case 'j':
            static_join = true;
            break;
        default:
            usage();
            exit(-1);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc < 1) {
        usage();
        exit(-1);
    }

    string myIP(argv[0]);

    /* Initialize Manager */
    Manager manager(myIP, 1);

    // If backend is static_joining, ask user to send neighbor updates
    if (static_join) {
        pthread_t localThread;
        cout << "Static joining.." << endl;
        int code = pthread_create(&localThread, NULL, ringStart, &manager);
    }

    pthread_t localServerThreadId_;
    int code = pthread_create(&localServerThreadId_, NULL,
                              localServerThreadLoop, &manager);

    pthread_join(localServerThreadId_, NULL);

    cout << "Exiting front-end manager." << endl;

    return 0;
}

void *localServerThreadLoop(void *p)
{
    Manager *m = (Manager*) p;

    int port = MANAGER_PORT_BASE;
    shared_ptr<Manager> handler(m);
    shared_ptr<TProcessor> processor(new FawnKVManagerProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return NULL;
}

void *ringStart(void* p) {
    Manager* m = (Manager*)p;
    string tempstr;
    cout << "Enter 'start' to setup neighbors." << endl;
    cin >> tempstr;
    m->sendNeighborUpdates();
    return NULL;
}

