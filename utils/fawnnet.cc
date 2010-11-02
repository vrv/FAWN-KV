/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <netinet/tcp.h>

#include "fawnnet.h"

using namespace std;

int fawn::connectTCP(string IP, int port)
{
    int sock;
    if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        cerr << "cannot create socket" << endl;
        return -1;
    }

    int yes = 1;
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
                   &yes, sizeof(int)) == -1)
        {
            perror("setsockopt");
            cerr << "connectTCP: error in setsockopt" << endl;
            return -1;
        }

    struct sockaddr_in serverAddress;
    struct hostent *he = gethostbyname(IP.data());
    if (he == NULL) {
        cout << "problem resolving host: " << IP.data() << endl;
        return -1;
    }
    bcopy(he->h_addr, (struct in_addr *)&(serverAddress.sin_addr), he->h_length);

    serverAddress.sin_family = he->h_addrtype;
    serverAddress.sin_port = htons(port);

    if (connect(sock, (struct sockaddr *)&serverAddress,
                sizeof serverAddress) == -1) {
        perror("connectTCP: connect failed");
        return -1;
    }

    return sock;
}

// Uses local hostname to find IP
string fawn::getMyIP()
{
    char hostname[128];
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    struct addrinfo *servinfo, *p;

    gethostname(hostname, sizeof(hostname));

    getaddrinfo(hostname, NULL, &hints, &servinfo);

    char ipstr[INET6_ADDRSTRLEN];
    for (p = servinfo; p != NULL; p = p->ai_next) {
        void* addr;
        if (p->ai_family == AF_INET) {
            struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
            addr = &(ipv4->sin_addr);
        } else {
            struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
            addr = &(ipv6->sin6_addr);
        }
        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
    }

    return string(ipstr);
}
