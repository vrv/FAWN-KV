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

#include "TFawnKV.h"
#include "fawnnet.h"
#include "hashutil.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace fawn;

pthread_cond_t client_cv;
pthread_mutex_t client_mutex;
bool has_data;
string data;
int64_t version_global;

void put_cb(unsigned int continuation)
{
    pthread_mutex_lock(&client_mutex);
    has_data = true;
    pthread_cond_signal(&client_cv);
    pthread_mutex_unlock(&client_mutex);
}

void put_w_cb(unsigned int continuation, int64_t version)
{
    pthread_mutex_lock(&client_mutex);
    has_data = true;
    version_global = version;
    pthread_cond_signal(&client_cv);
    pthread_mutex_unlock(&client_mutex);
}

void get_cb(const DBID& p_key, const string& p_value, unsigned int continuation, bool success)
{
    pthread_mutex_lock(&client_mutex);
    has_data = true;
    data = p_value;
    pthread_cond_signal(&client_cv);
    pthread_mutex_unlock(&client_mutex);
}

FawnKVClt::FawnKVClt(const std::string& managerIP, const std::string& clientIP, const int32_t clientPort) {
    pthread_mutex_init(&client_mutex, NULL);
    pthread_cond_init(&client_cv, NULL);
    has_data = false;

    if (clientIP != "")
        myIP = clientIP;

    if (clientPort == 0)
        myPort = FE_SERVER_PORT_BASE;
    else
        myPort = clientPort;

    /* Initialize ring */
    frontend = new FrontEnd(managerIP, myIP, myPort);
    frontend->register_put_cb((void (*)(unsigned int)) &put_cb);
    frontend->register_put_w_cb((void (*)(unsigned int, int64_t)) &put_w_cb);
    frontend->register_get_cb((void (*)(const fawn::DBID&, const std::string&, unsigned int, bool)) &get_cb);


}

FawnKVClt::~FawnKVClt() {
    pthread_mutex_destroy(&client_mutex);
    pthread_cond_destroy(&client_cv);
    delete frontend;
}

//Currently, we're hashing keys so we avoid the 4byte issue ("user" == "user1" == "user2")
//This will prevent range queries / textual keys

//string FawnKVClt::get(const std::string& key) {
string FawnKVClt::get(const std::string& key_long) {
    u_int32_t key_int = HashUtil::BobHash(key_long);
    string key = string((const char *) &key_int, 4);

    try {
	frontend->get(key, continuation);
	continuation++;
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }

    //note: locks are used for signalling only, each FawnKVClt is single execution only
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
	frontend->put(key, value, continuation);
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

//void FawnKVClt::put_w(const std::string& key, const std::string& value) {
int64_t FawnKVClt::put_w(const std::string& key_long, const std::string& value) {
    u_int32_t key_int = HashUtil::BobHash(key_long);
    string key = string((const char *) &key_int, 4);

    try {
	frontend->put_w(key, value, continuation);
	continuation++;
    } catch (TException &tx) {
	fprintf(stderr, "Transport error: %s\n", tx.what());
    }
    pthread_mutex_lock(&client_mutex);
    while (!has_data)
	pthread_cond_wait(&client_cv, &client_mutex);
    has_data = false;
    int64_t ret_version = version_global;
    pthread_mutex_unlock(&client_mutex);

    return ret_version;
}

void FawnKVClt::remove(const std::string& key) {
    try {
	frontend->remove(key, continuation);
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

