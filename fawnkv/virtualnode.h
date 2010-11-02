/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _VIRTUALNODE_H_
#define _VIRTUALNODE_H_

#include "dbid.h"
#include "node.h"

using fawn::DBID;

class VirtualNode
{
public:
    Node *n;
    DBID* id;
    bool dead;
    VirtualNode ();
    VirtualNode (Node *nde, DBID *a);
    ~VirtualNode();

    bool operator==(const VirtualNode &rhs) const;
};

#endif
