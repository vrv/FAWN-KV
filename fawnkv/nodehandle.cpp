/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
using namespace std;
#include <string>
#include <algorithm>
#include "dbid.h"
#include "node.h"
#include "nodehandle.h"
#include "print.h"
using fawn::DBID;

NodeHandle::NodeHandle(string myID) : myID(myID)
{
}

pair<Node*, int> NodeHandle::retrievePutTarget() {
  Node* target = owner;
#ifdef DEBUG
  cout << "Owner is "; target->print();
  cout << "valid is " << (target->valid ? "true" : "false") << endl;
#endif
  uint i = 0;
  while(!target->valid && i < replicaGroup.size())
  {
    target = replicaGroup[i];
    i++;
#ifdef DEBUG
    cout << "Target is "; target->print();
    cout << "valid is " << (target->valid ? "true" : "false") << endl;
#endif
  }
  if ( (i != 0) && (i == replicaGroup.size()) )
    target = NULL;

  return pair<Node*, int>(target, i);
}

Node* NodeHandle::retrieveGetTarget(bool interimTail) {
  Node* target = NULL;
  //cout << "interim tail = " << (interimTail ? "true" : "false") << endl;
  if (interimTail && replicaGroup.back()->valid == false) {
    if (replicaGroup.size() >= 2) {
        target = replicaGroup[replicaGroup.size()-2];
        int i = replicaGroup.size()-2;
        while(!target->valid && i >= 0)
        {
          target = replicaGroup[i];
          i--;
        }
    } else if (replicaGroup.size() == 1) {
        target = replicaGroup[0];
        if (!target->valid) {
          target = owner;
        }
    } else { // replicaGroup.size() == 0
      target = owner; // R = 1, there is no interim tail to worry about.
    }
  } else if (replicaGroup.size() > 0) {
    target = replicaGroup.back();
    int i = replicaGroup.size()-1;
    while(!target->valid && i >= 0)
    {
      target = replicaGroup[i];
      i--;
    }
    //cout << "valid flag is " << target->valid << endl;
    //cout << "i is " << i << endl;
  } else {
    target = owner;
  }

  //cout << "Target is "; target->print();

  return target;
}


int NodeHandle::put(DBID* key, string* value, uint64_t continuation, bool remove, uint32_t sequence, bool interimTail)
{
    if (owner->isConnected()) {
        cerr << "Cannot connect to replica group owner in put\n" << endl;
        exit(1);
    }
#ifdef DEBUG
    cout << "Sending Put Request" << endl;
    cout << "IP: " << owner->IP << endl;
    cout << "IPsize: " << owner->IP.size() << endl;
    cout << "Keysize: " << key->size() << endl;
    cout << "Key: ";
    key->printValue();
    cout << endl;
    cout << "Valuesize: " << value->size() << endl;
    //cout << "Value: " << value->c_str() << endl;
#endif
    pair<Node*, int> p = retrievePutTarget();
    Node* toSend = p.first;
    int i = p.second;
    assert (toSend != NULL);

    //if we're operating at one below capacity (node leave, for example
//    int delta = (interimTail ? 1 : 0);

    string skey(key->data(), key->size());
    string svalue(value->data(), value->size());
    try {
        //cout << "Putting " << bytes_to_hex(skey) << ", seq# " << sequence << ", continuation " << continuation << " to " << toSend->IP << ":" << toSend->port  << endl;

        toSend->backend->put(skey, svalue, replicaGroup.size() + 1 - i , replicaGroup.size() +1 - i , continuation, sequence, false, remove, myID);
    }
    catch(...) {
        std::cout << "Put Failed" << endl;
        return -1;
    }
    return 0;
}

/* Sends request to tail node */
int NodeHandle::get(DBID* key, uint64_t continuation, bool interimTail)
{
    Node* target = retrieveGetTarget(interimTail);
    assert(target != NULL);

    if (target->isConnected() ) {
        cerr << "Cannot connect to replica group owner in put\n" << endl;
        exit(1);
    }
#ifdef DEBUG
    cout << "Sending Get Request" << endl
         << " IP: " << target->IP << endl
         << " IPsize: " << target->IP.size() << endl
         << " Keysize: " << key->size() << endl
         << " Key: ";
    key->printValue();
    cout << endl;
#endif

    string skey(key->data(), key->size());
    try {
        //cout << "Getting " << bytes_to_hex(skey) << " from " << target->IP << ":" << target->port  << endl;
      target->backend->get(skey, continuation, myID);
    }
    catch(...) {
        cout << "Failed get" << endl;
        return -1;
    }


    return 0;
}

void NodeHandle::setOwner(Node* n)
{
    owner = n;
}


Node* NodeHandle::getOwner()
{
    return owner;
}

int NodeHandle::addReplica(Node* n)
{
    /* Owner must be first, then replicas in clockwise order */
    /* XXX clockwise or counterclockwise??? */
    replicaGroup.push_back(n);
    return 0;
}

void NodeHandle::print() {
    for_each(replicaGroup.begin(), replicaGroup.end(), mem_fun(&Node::print));
}
