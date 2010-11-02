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
#include "ring.h"

using namespace std;

using fawn::DBID;

Ring::Ring() {

}

Ring::~Ring() {

}

void Ring::UpdateState(ringState rs)
{
    // Naive implementation throws everything out and
    // starts from scratch.
    list<VirtualNode*>::iterator vnit;
    for (vnit = nodeList.begin(); vnit != nodeList.end(); vnit++) {
        delete *vnit;
    }
    nodeList.clear();
    vid_temptail_map.clear();
    vid_shorter_map.clear();

    // Assumption: rs node list is in increasing order
    for (uint i = 0; i < rs.nodes.size(); i++) {
        NodeData nd = rs.nodes[i];
        string nodeID = getID(rs.nodes[i].ip, rs.nodes[i].port);
        Node *n = createNode(rs.nodes[i].ip, rs.nodes[i].port);
        DBID id(rs.nodes[i].VnodeID);
        VirtualNode *vn = new VirtualNode(n, &id);
        nodeList.push_back(vn);
    }

    replica_group_size = rs.replication;

    for (uint i = 0; i < rs.temptails.size(); i++) {
        vid_temptail_map[rs.temptails[i]] = true;
    }

    for (uint i = 0; i < rs.shorters.size(); i++) {
        vid_shorter_map[rs.shorters[i]] = true;
    }

    return;
}

string Ring::getRangeKey(string key) {
    DBID k(key);
    list<VirtualNode*>::const_iterator i;
    if (!nodeList.empty()) {
        for (i = nodeList.begin(); i != nodeList.end(); i++) {
            DBID* p_id = (*i)->id;
            if ( (k < *p_id) || (k == *p_id) ) {
                return p_id->actual_data_str();
            }
        }

        /* If we didn't find owner in previous loop */
        /* then the owner is the first node*/
        if (i == nodeList.end()) {
            i = nodeList.begin();
            return (*i)->id->actual_data_str();
        }
    }
    return "";
}

DBID* Ring::getSuccessorID(string key)
{
    DBID vn(key);
    VirtualNode* p_succ = NULL;

    list<VirtualNode*>::const_iterator i, j;

    // nodelist in ascending order by ID
    for (i = nodeList.begin(); i != nodeList.end(); i++) {
        j = i;
        j++;

        if (j == nodeList.end()) {
            j = nodeList.begin();
        }

        if ( *((*i)->id) == vn ) {
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
            if (between((*i)->id, (*j)->id, &vn)) {
                p_succ = *j;
                break;
            }
        }
    }

    return p_succ->id;
}

NodeHandle* Ring::getReplicaGroup(string key) {
#ifdef DEBUG
    cout << "[GRG] Looking for key: " <<  bytes_to_hex(k->actual_data_str()) << endl;
#endif

    DBID k(key);
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
            if ( (k < *p_id) || (k == *p_id) ) {
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

        uint uniqueReplica = 1;

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

    return nh;
}


// no locking here - caller locks nodelist
void Ring::printNodes() {
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


Node* Ring::createNode(string ip, const int32_t port)
{
    // Check if you already have a connection to the node
    string id = getID(ip, port);
    if (ip_node_map.find(id) == ip_node_map.end()) {
        Node *n = new Node(ip, port, false);
        ip_node_map[id] = n;
        return n;
    } else {
        return ip_node_map[id];
    }
}
