/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string>
#include <iostream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TSocket.h>


#include "dbid.h"
#include "node.h"
#include "ring.h"

using fawn::DBID;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace boost;

Node::Node() :
    IP(""),
    connectStatus(0), cardiacState(ALIVE),
    joinState(NONE), beatsMissed(0)
{
    valid =true;
}

Node::Node(string ip, int p, bool connect) :
    connectStatus(0), cardiacState(ALIVE),
    joinState(NONE), beatsMissed(0)
{
    IP = ip;
    port = p;
    valid =true;
    connected = connect;

    if (connect) {
        // Connect to ip
        shared_ptr<TTransport> socket(new TSocket(IP.data(), port));
        shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        be_transport = transport;
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(be_transport));
        backend = new FawnKVBackendClient(protocol);

        while (1) {
            try {
                be_transport->open();
                break;
            } catch (TException &tx) {
                fprintf(stderr, "Transport error: %s\n", tx.what());
            }
            cout << "Sleeping" << endl;
            sleep(1);
        }
    }

}

Node::~Node() {
    cout << "deleting node" <<  endl;

    if (connected) {
        try {
            be_transport->close();
        } catch (TException &tx) {
            fprintf(stderr, "Transport error: %s\n", tx.what());
        }

        delete backend;
    }
}

void Node::addVnode(const DBID id) {
    DBID vid = id;
    if(vnodes.empty()) {
        vnodes.push_front(vid);
        return;
    }

    list<DBID>::iterator i;

    // Add node to list in ascending order by ID
    for (i = vnodes.begin(); i != vnodes.end(); i++) {
        if ( vid < *i) {
            vnodes.insert(i, vid);
            return;
        } else if ( *i == vid ) {
            cerr << "Ring::addNodetoRing failed, ID already exists"<<endl;
            return;
        }
    }
    vnodes.push_back(vid);
}

void Node::setAlive() {
    // benign race
    beatsMissed = 0;
    cardiacState = ALIVE;
}

void Node::setDead(){
    cardiacState = DEAD;
}

char Node::isConnected() {
    return connectStatus;
}

void Node::print() {
    cout << IP << ":" << port << endl;
}

void Node::printStatus() {

    cout << "Physical Node IP: " << IP << endl <<
        "\t Status: ";

    switch(cardiacState) {
    case ALIVE:
        cout << "ALIVE" << endl;
        break;
    case ARRHYTHMIA:
        cout << "ARRHYTHMIA" << endl;
        cout << "\t Beats Missed: " << beatsMissed << endl;
        break;
    case DEAD:
        cout << "DEAD" << endl;
        break;
    default:
        cout << "error" << endl;
    }

    if (connectStatus == 1) {
        cout << "\tConnected" << endl;
    } else {
        cout << "\tDisconnected" << endl;
    }
}


bool Node::operator==(const Node &rhs) const {
    // string compare returns 0 if equal
    if ( (IP.compare(rhs.IP) == 0) &&  (port == rhs.port) )
        return true;
    else
        return false;

}

bool Node::operator<(const Node &rhs) const {
    // string compare returns - if <
    if (IP.compare(rhs.IP) < 0)
        return true;
    else if (IP.compare(rhs.IP) == 0)
        return (port < rhs.port) ? true : false;
    else
        return false;
}

void Node::increment_beats_missed() {
    // benign race
    beatsMissed++;
}

int Node::get_beats_missed() {
    // benign race
    return beatsMissed;
}
