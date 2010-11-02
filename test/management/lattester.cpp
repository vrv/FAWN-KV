using namespace std;

#include <iostream>
#include <sstream>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <cstdlib>
#include <stdint.h>
#include <unistd.h>
#include <map>
#include <list>
#include <pthread.h>

#include "fe.h"
#include "dbid.h"
#include "hashutil.h"
#include "timing.h"

using fawn::HashUtil;
using fawn::DBID;

////////////////////////////////////////////////////////////////////////////////

int totalGets = 1500000;
int numGets = 0;
vector<uint32_t> *lt1 = new vector<uint32_t>;
vector<uint32_t> *lt2 = new vector<uint32_t>;
bool p = false;

void put_cb(unsigned int continuation)
{
    //cout << "Woah! Got to put_cb." << endl;
    //cout << "put_cb: continuation = " << continuation << endl;
    if (p) {
	if (continuation == 0)
	    cout << "Done rewrite" << endl;

        struct timeval t2;
        struct timeval t1;
        t1.tv_sec = (*lt1)[continuation];
        t1.tv_usec = (*lt2)[continuation];
        gettimeofday(&t2, NULL);
        cout << continuation << "\t" << timeval_diff(&t1, &t2) << endl;
    }
}

void get_cb(const DBID& p_key, const string& p_value, unsigned int continuation, bool success)
{
    /*
    if (p_value) {
        cout << "\t Value: " << p_value->c_str() << endl;
    }
    else {
        cout << "\t Value: NULL" << endl;
    }
    */


#ifdef DEBUG
    cout << "\t Continuation Id: " << continuation << endl;
    cout << "\t Get Success?: " << (success ? "yes!" : "no") << " Success #: " ;
#endif

    if (success) {
        if (p) {
            struct timeval t2;
            struct timeval t1;
            t1.tv_sec = (*lt1)[continuation];
            t1.tv_usec = (*lt2)[continuation];
            gettimeofday(&t2, NULL);
            cout << continuation << "\t" << timeval_diff(&t1, &t2) << endl;
        }

        numGets++;
    } else {
        cout << "boo: " << continuation << endl;
    }

    if (numGets == totalGets) {
        cout << "YAY!" << endl;
    }
}


void *localThreadLoop(void *p)
{
    // TODO: Fix Hack. Just used to keep client around.
    while(1) {
        sleep(10);
    }
    return NULL;
}

//CryptoPP::SHA1 sha;

void usage()
{
    cerr << "./lattester <Manager IP> <My IP> <My Port> <msg_size_in_bytes=1024>\n" <<endl;
}

void maintenance_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 80000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;
    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );

    uint maxKey = 500000;
    //uint maxKey = 2000000;
    string value(msg_size, 'b');
    for (uint i = 0; i < maxKey; i++) {
        uint num = i;//rand()%10000000;
    	if (num % 5000000 == 0) {
    	    cout << "iKey = " << num << endl;
    	    nanosleep(&req, NULL);
    	}
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));


        //cout << "Sending a put" << endl;
        // Send a Put
        fe->put(key, value, num);
    	nanosleep(&req, NULL);
        //cout << "Getting ..." << endl;
        //ring.get(key, 2);

    }


    struct timeval t;
    req.tv_sec = 0;
    req.tv_nsec = 2000000;

    int cont = 0;
    cout << "Type start to begin performing gets." << endl;
    cin >> tempstr;
    cout << "Starting gets..." << endl;

    vector<uint32_t> *l  = new vector<uint32_t>;


    //uint32_t numGetsReq = 1000000;


    // Randomize gets
    for (uint32_t i = 0; i < 5*maxKey; i++) {
	l->push_back(rand() % maxKey);
    }

    string value_put(msg_size, 'b');
    struct timeval start;


    int num = 0;
    string ps_key((const char*)&num, sizeof(num));
    uint32_t key_id = HashUtil::BobHash(ps_key);
    string key((char *)&key_id, sizeof(uint32_t));
    double val = ((double)num / (double)maxKey);
    lt1->reserve(5*maxKey);
    lt2->reserve(5*maxKey);
    p = true;
    gettimeofday(&start, NULL);
    double ratio = 0.0;

    {

    	for (uint i = 0; i < 5*maxKey; i++) {
	    nanosleep(&req, NULL);
	    // start time
	    if (val < ratio) {
		fe->put(key, value_put, 2);
	    } else {
		// disable split if i == -1
		if (i == 5000) {
		    printf("Split req\n");
		    int num2 = 0;
		    string ps_key2((const char*)&num2, sizeof(num2));
		    uint32_t key_id2 = HashUtil::BobHash(ps_key2);
		    string key2((char *)&key_id2, sizeof(uint32_t));
		    sleep(2);
		    fe->get(key2, (uint64_t)-1);
                } else {
		    //gettimeofday(&t, NULL);
		    int num2 = (*l)[i];
		    string ps_key2((const char*)&num2, sizeof(num2));
		    uint32_t key_id2 = HashUtil::BobHash(ps_key2);
		    string key2((char *)&key_id2, sizeof(uint32_t));
                    gettimeofday(&t, NULL);
                    lt1->push_back(t.tv_sec);
                    lt2->push_back(t.tv_usec);
		    fe->get(key2, cont);
		    //fe->put(key2, &value, cont);
		    //cout << cont << "\t" << t.tv_sec << "\t" << t.tv_usec << endl;
		    cont++;
                }
	    }


    	}
    }

    cout << "Done with gets..." << endl;

    return;
}

void put_test(FrontEnd *fe, int msg_size)
{

    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 150000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;
    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );


    uint maxKey = 10000000;
    //lt1->reserve(maxKey);
    //lt2->reserve(maxKey);
    string value(msg_size, 'b');
    for (uint i = 0; i < maxKey; i++) {
        uint num = i;
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));
        //struct timeval t;
        //gettimeofday(&t, NULL);
        //lt1->push_back(t.tv_sec);
        //lt2->push_back(t.tv_usec);
        fe->put(key, value, i);
        //nanosleep(&req, NULL);
    }

    cout << "Enter 'start' to end test." << endl;
    cin >> tempstr;

    return;
}

void putget_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 50000;
    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;    
    cout << "Enter 'start' to send neighbors." << endl;
    cin >> tempstr;
    p = true;


    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );


    double ratio = 1;
    p = true;

    uint maxKey = 1000000;
    lt1->reserve(maxKey);
    lt2->reserve(maxKey);

    // Randomize
    vector<uint32_t> *l  = new vector<uint32_t>;

    for (uint32_t i = 0; i < 5*maxKey; i++) {
	l->push_back(rand() % maxKey);
    }


    string value(msg_size, 'b');
    for (uint i = 0; i < maxKey; i++) {
        nanosleep(&req, NULL);
        uint num = (*l)[i];
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));
        struct timeval t;
        gettimeofday(&t, NULL);
        lt1->push_back(t.tv_sec);
        lt2->push_back(t.tv_usec);
        double v = (double)((double)rand() / (double)RAND_MAX);
        if (v < ratio)
            fe->put(key, value, i);
        else {
            fe->get(key, i);
        }

    }

    cout << "Enter 'start' to end test." << endl;
    cin >> tempstr;

    return;
}

void get_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 200000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;
    srand ( time(NULL) );

    uint maxKey = 10000000;

    cout << "Enter 'start' to start performing gets." << endl;
    cin >> tempstr;

    for (uint i = 0; i < maxKey; i++) {
        uint num = i;
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));


        fe->get(key, i);
    }

    cout << "Done with gets..." << endl;

    return;
}

void join_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 70000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;

    //cout << "Enter 'start' to end test." << endl;
    //cin >> tempstr;
    cout << "Enter 'start' to start performing gets." << endl;
    cin >> tempstr;

    // Randomize gets
    vector<uint32_t> *l  = new vector<uint32_t>;

    uint maxKey = 10000000;

    for (uint32_t i = 0; i < 5*maxKey; i++) {
	l->push_back(rand() % maxKey);
    }

    for (uint i = 0; i < 5*maxKey; i++) {
        nanosleep(&req, NULL);
        uint num = (*l)[i];
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));


        fe->get(key, i);
    }

    cout << "Done with gets..." << endl;

    return;
}


void simple_test(FrontEnd *fe, int msg_size)
{
    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    sleep(5);
    uint32_t key_id_1 = 1090387202;
    //num = 4089352983;
    string key1((char *)&key_id_1, sizeof(uint32_t));
    //key1->printValue();


    uint32_t key_id_2 = 395558643;
    //num = 50199872;
    string key2((char *)&key_id_2, sizeof(uint32_t));
    //key2->printValue();



    int maxKey = 2;
    for (int i = 0; i < maxKey; i++) {
        string value(msg_size, 'b');
        cout << "Sending a put" << endl;

        // Send a Put
        if (i%2 == 0) {
            fe->put(key1, value, i);
        } else {
            fe->put(key2, value, i);
        }

    }

    sleep(5);
    numGets = 0;
    maxKey = totalGets;
    int j = 0;
    for (int i = 0; i < maxKey; i++) {
        //cout << "Getting ..." << endl;
        //struct timeval t2;
        //gettimeofday(&t2, NULL);
        //cout << i << "\t" << t2.tv_sec << "\t" << t2.tv_usec << endl;

       if (j == 0) {
            fe->get(key1, i);
        } else {
            fe->get(key2, i);
        }
       j++;
       if (j == 2) j = 0;

    }
    cout << "done!" << endl;

    return;
}

int main(int argc, char **argv)
{

    if (argc < 2) {
        usage();
        exit(-1);
    }

    string managerIP(argv[1]);
    string myIP(argv[2]);
    int myPort = atoi(argv[3]);

    int msg_size = 1024;
    if (argc > 3) {
       msg_size  = atoi(argv[4]);
    }

    cout << "msg_size = " << msg_size << endl;
    /* Initialize ring */
    FrontEnd fe(managerIP, myIP, myPort);

    pthread_t localThreadId_;
    pthread_create(&localThreadId_, NULL,
                   localThreadLoop, (void*)&fe);


    //join_test(&ring, msg_size);
    //putget_test(&ring, msg_size);
    maintenance_test(&fe, msg_size);
    //simple_test(&ring, msg_size);

    pthread_join(localThreadId_, NULL);
    cout << "Exiting front-end manager." << endl;

    return 0;
}
