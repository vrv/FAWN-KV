/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _NODEHANDLE_H_
#define _NODEHANDLE_H_

using namespace std;
#include <deque>
#include "dbid.h"
#include "node.h"
#include <string>
#include <tbb/atomic.h>
using fawn::DBID;

class NodeHandle
{



public:


    NodeHandle(string myID = "");

    int put(DBID* key, string* value, uint64_t continuation, bool remove, uint32_t sequence=0, bool interimTail=false);
    int get(DBID* key, uint64_t continuation, bool interimTail);

    pair<Node*, int> retrievePutTarget();
    int retrievePutTargetIndex();
    Node* retrieveGetTarget(bool interimTail);

    void setOwner(Node* n);
    int addReplica(Node *n);

    void print();

    //added by jhferris for the leave protocol
    DBID* getRangeKey();

    Node* getOwner();
    std::deque<Node*> replicaGroup;
private:
    Node* owner;
    string myID;

};

#endif
