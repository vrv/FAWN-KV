/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "virtualnode.h"
#include "node.h"

using fawn::DBID;

VirtualNode::VirtualNode() :
    n(NULL), id(NULL), dead(false){
}

VirtualNode::VirtualNode(Node *nde, DBID *a) :
    n(nde), id(new DBID(a->data(), a->size())), dead(false) {
}

VirtualNode::~VirtualNode() {
    delete id;
}


bool VirtualNode::operator==(const VirtualNode &rhs) const {
    return rhs.id == id;
}
