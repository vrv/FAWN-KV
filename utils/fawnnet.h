/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _NET_H_
#define _NET_H_

using namespace std;
#include <iostream>
#include <cstdlib>

#include <stdio.h>

namespace fawn {
    int connectTCP(string IP, int port);
    string getMyIP();
}
#endif
