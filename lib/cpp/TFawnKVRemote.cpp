/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <transport/TBufferTransports.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TSocket.h>

#include "TFawnKVRemote.h"
#include "fawnnet.h"
#include "hashutil.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace fawn;

void *serverStart(void* p) {
    TSimpleServer *srv = (TSimpleServer*)p;
    srv->serve();
    return NULL;
}

FawnKVClt::FawnKVClt(const std::string& frontendIP, const int32_t port, const std::string& clientIP, const int32_t clientPort) {
    pthread_mutex_init(&client_mutex, NULL);
    pthread_cond_init(&client_cv, NULL);
    has_data = false;

    if (clientIP == "")
        myIP = getMyIP();
    else
        myIP = clientIP;

    if (clientPort == 0)
        myPort = port+1;
    else
        myPort = clientPort;

    shared_ptr<TTransport> socket(new TSocket(frontendIP.data(), port));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    c = new FawnKVClient(protocol);
    continuation = 0;

    try {
	transport->open();

	shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	shared_ptr<FawnKVClientHandler> handler(new FawnKVClientHandler(this));
	shared_ptr<TProcessor> processor(new FawnKVAppProcessor(handler));
        shared_ptr<TServerTransport> serverTransport(new TServerSocket(myPort));
	shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

	server = new TSimpleServer(processor, serverTransport,transportFactory, protocolFactory);

	pthread_t localThread;
	int code = pthread_create(&localThread, NULL, serverStart, server);

	sleep(1);

	cid = c->init(myIP, myPort);
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }

}

FawnKVClt::~FawnKVClt() {
    pthread_mutex_destroy(&client_mutex);
    pthread_cond_destroy(&client_cv);
    delete server;
    delete c;
}

//Currently, we're hashing keys so we avoid the 4byte issue ("user" == "user1" == "user2")
//This will prevent range queries / textual keys

//string FawnKVClt::get(const std::string& key) {
string FawnKVClt::get(const std::string& key_long) {
    u_int32_t key_int = HashUtil::BobHash(key_long);
    string key = string((const char *) &key_int, 4);

    try {
	c->get(key, continuation, cid);
	continuation++;
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }

    pthread_mutex_lock(&client_mutex);
    while (!has_data)
	pthread_cond_wait(&client_cv, &client_mutex);
    has_data = false;
    pthread_mutex_unlock(&client_mutex);
    return data;
}

//void FawnKVClt::put(const std::string& key, const std::string& value) {
void FawnKVClt::put(const std::string& key_long, const std::string& value) {
    u_int32_t key_int = HashUtil::BobHash(key_long);
    string key = string((const char *) &key_int, 4);

    try {
	c->put(key, value, continuation, cid);
	continuation++;
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }
    pthread_mutex_lock(&client_mutex);
    while (!has_data)
	pthread_cond_wait(&client_cv, &client_mutex);
    has_data = false;
    pthread_mutex_unlock(&client_mutex);
}

void FawnKVClt::remove(const std::string& key) {
    try {
	c->remove(key, continuation, cid);
	continuation++;
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }
    pthread_mutex_lock(&client_mutex);
    while (!has_data)
	pthread_cond_wait(&client_cv, &client_mutex);
    has_data = false;
    pthread_mutex_unlock(&client_mutex);
}


FawnKVClientHandler::FawnKVClientHandler(FawnKVClt *f) {
    fc = f;
}

void FawnKVClientHandler::get_response(const std::string& value, const int64_t continuation) {
    pthread_mutex_lock(&fc->client_mutex);
    fc->has_data = true;
    fc->data = value;
    pthread_cond_signal(&fc->client_cv);
    pthread_mutex_unlock(&fc->client_mutex);
}

void FawnKVClientHandler::put_response(const int64_t continuation) {
    pthread_mutex_lock(&fc->client_mutex);
    fc->has_data = true;
    pthread_cond_signal(&fc->client_cv);
    pthread_mutex_unlock(&fc->client_mutex);
}

void FawnKVClientHandler::remove_response(const int64_t continuation) {
    pthread_mutex_lock(&fc->client_mutex);
    fc->has_data = true;
    pthread_cond_signal(&fc->client_cv);
    pthread_mutex_unlock(&fc->client_mutex);
}
