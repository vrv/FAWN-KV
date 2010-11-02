/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _NODE_H_
#define _NODE_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string>
#include <transport/TTransportUtils.h>

#include "dbid.h"
#include "FawnKVFrontend.h"
#include "FawnKVBackend.h"

using namespace fawn;
using namespace boost;
using namespace apache::thrift::transport;

enum health_status {ALIVE, ARRHYTHMIA, DEAD};
enum join_type {NONE, STATIC, REQUEST, PUT, PUTGET};

class Node
{
public:
    FawnKVBackendClient *backend;

    //used for marking a node has failed without removing it from the phyNodeList, just yet
    bool valid;
    string IP;
    int port;

    std::list<DBID> vnodes;

    Node ();
    Node (string ip, int p, bool connect = true);
    ~Node();

    void addVnode(const DBID id);

    void increment_beats_missed();
    int get_beats_missed();
    void setAlive();
    void setDead();
    //int  connectToNode();
    void timerCallback() {};
    void print();
    char isConnected();

    /* Print management info. */
    void printStatus();

    bool operator==(const Node &rhs) const;
    bool operator<(const Node &rhs) const;

private:
    char connectStatus;
    health_status cardiacState;
    join_type joinState;
    int beatsMissed;
    shared_ptr<TTransport> be_transport;
    bool connected;
};

#endif

