/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <signal.h>
#include </usr/include/semaphore.h>

#include "fe.h"
#include "dbid.h"
#include "hashutil.h"
#include "timing.h"
#include <tbb/atomic.h>

using fawn::HashUtil;
using fawn::DBID;

////////////////////////////////////////////////////////////////////////////////

int totalGets = 1500000;
int numPutsLeft = 0;
int numGets = 0;
uint maxKey = 2000;
uint numOps = 0;
vector<uint32_t> *lt1 = new vector<uint32_t>;
vector<uint32_t> *lt2 = new vector<uint32_t>;
bool consistency_test = false;
vector<string> put_values;
bool p = false;
atomic<uint32_t> outstanding;
bool b_done = false;
struct timeval start_time;
struct timeval end_time;
sem_t barrier;

void put_cb(unsigned int continuation)
{
    //cout << "Woah! Got to put_cb." << endl;
    //cout << "put_cb: continuation = " << continuation << endl;
    outstanding--;
    if (p) {
        struct timeval t2;
        struct timeval t1;
        t1.tv_sec = (*lt1)[continuation];
        t1.tv_usec = (*lt2)[continuation];
        gettimeofday(&t2, NULL);
        cout << continuation << "\t" << timeval_diff(&t1, &t2) << endl;
    }
    numPutsLeft--;
    if (numPutsLeft == 0) {
      gettimeofday(&end_time, NULL);
      cout << "time for puts to complete: " << "\t" << timeval_diff(&start_time, &end_time) << endl;
      // signal semaphore
      sem_post(&barrier);
    }
}

void get_cb(const DBID& p_key, const string& p_value, unsigned int continuation, bool success)
{
  outstanding--;
#ifdef DEBUG
    /*
    if (p_value) {
        cout << "\t Value: " << p_value->c_str() << endl;
    }
    else {
        cout << "\t Value: NULL" << endl;
    }
    */

    cout << "\t Continuation Id: " << continuation << endl;
    cout << "\t Get Success?: " << (success ? "yes!" : "no") << " Success #: " ;
#endif

    numGets++;
    if (success) {
        //struct timeval t2;
        //gettimeofday(&t2, NULL);
	//cout << continuation << endl;
        //cout << continuation << "\t" << t2.tv_sec << "\t" << t2.tv_usec << endl;
        if (consistency_test) {
            //cout << "Got: " << p_value << endl;
            //cout << "Expected: " << put_values[continuation] << endl;
            assert(p_value == put_values[continuation]);
        }
            //assert(put_values[continuation] == p_value);
    } else {
        cout << "boo: " << continuation << endl;
    }

    if (numGets == totalGets) {
        cout << "YAY!" << endl;
        gettimeofday(&end_time, NULL);
        cout << "time for gets to complete: " << "\t" << timeval_diff(&start_time, &end_time) << endl;
        // signal semaphore
        sem_post(&barrier);
    }
}


void put_cb2(unsigned int continuation)
{
    // signal semaphore
    sem_post(&barrier);
}

void get_cb2(const DBID& p_key, const string& p_value, unsigned int continuation, bool success)
{
    if (success)
        printf("Success\n");
    else
        printf("Failure\n");

    cout << "Got value: " << p_value << endl;
    sem_post(&barrier);
}

void sighandler(int sig) {
    cout << "Signal " << sig << " caught..." << endl;
    b_done = true;
}

void *localThreadLoop(void *p)
{
    // TODO: Fix Hack. Just used to keep client around.
    while(!b_done) {
        sleep(1);
    }
    cout << "Exiting localThreadLoop" << endl;
    return NULL;
}

void usage()
{
    cerr << "./ringtester [-pgbmsij] [-n #] [-k #] [-r #] <managerIP> <My IP> <My Port> <msg_size_in_bytes=1024>\n" <<endl;

    cerr <<   "   -p         put test\n"
         <<   "   -g         get test\n"
         <<   "   -G         reget test\n"
         <<   "   -b         put/get test\n"
         <<   "   -m         maintenance test\n"
         <<   "   -s         simple test\n"
         <<   "   -i         join test\n"
         <<   "   -d         put/get/delete/get test\n"
         <<   "   -n #       number of ops in test\n"
         <<   "   -k #       keyRange (size of db)\n";
}

void maintenance_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 200000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;
    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );

    string value(msg_size, 'b');
    for (uint i = 0; i < maxKey; i++) {
        uint num = i;
    	if (num % 5000000 == 0) {
    	    cout << "iKey = " << num << endl;
    	    nanosleep(&req, NULL);
    	}
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));
        //DBID* key = new DBID((char *)&key_id, sizeof(uint32_t));


        //cout << "Sending a put" << endl;
        // Send a Put
        fe->put(key, value, num);
    	//nanosleep(&req, NULL);
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

    gettimeofday(&start, NULL);
    double ratio = 0.0;

    {

    	for (uint i = 0; i < 5*maxKey; i++) {
	    //nanosleep(&req, NULL);
	    // start time
	    if (val < ratio) {
		fe->put(key, value_put, 2);
	    } else {
		// disable split if i == -1
		if (i == 50000) {
		    printf("Split req\n");
		    int num2 = 0;
		    string ps_key2((const char*)&num2, sizeof(num2));
		    uint32_t key_id2 = HashUtil::BobHash(ps_key2);
		    string key2((char *)&key_id2, sizeof(uint32_t));
		    sleep(2);
		    //ring->split(key2, num);
                } else {
		    //gettimeofday(&t, NULL);
		    int num2 = (*l)[i];
		    string ps_key2((const char*)&num2, sizeof(num2));
		    uint32_t key_id2 = HashUtil::BobHash(ps_key2);
                    string key2((char *)&key_id2, sizeof(uint32_t));
		    //double d = t.tv_sec * 1000000 + t.tv_usec;
		    //l->push_back(t.tv_sec);
		    //l2->push_back(t.tv_usec);
		    //(*l)[cont] = t.tv_sec;
		    //(*l2)[cont] = t.tv_usec;
		    //printf("Getting\n");
		    //l[cont] = t.tv_sec;
		    //l2[cont] = t.tv_usec;
		    fe->get(key2, cont);
		    //ring->put(key2, &value, cont);
		    //cout << cont << "\t" << t.tv_sec << "\t" << t.tv_usec << endl;
		    cont++;
		}
	    }


	    // //double d2 = t2.tv_sec * 1000000 + t2.tv_usec;
	    // //double d1 = (*l)[continuation];
	    // struct timeval t1;
	    // gettimeofday(&t1, NULL);

	    //ratio -= (0.1/300000);
	    // if (i > 300000 && i % 300000 == 0) {
	    // 	ratio += 0.1;
	    // 	printf("Increasing ratio: %f\n", ratio);
	    // 	if (ratio < 0)
	    // 	    ratio = 0;
	    // 	fflush(stdout);
	    // 	//sleep(5);
	    // }

    	}
    }


    cout << "Done with gets..." << endl;

    return;
}

void put_test(FrontEnd *fe, int msg_size)
{
    printf("Running Put test!\n");
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 1000000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;
    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );

    printf("Maxkey is %u", maxKey);
    //lt1->reserve(maxKey);
    //lt2->reserve(maxKey);
    bool skip = false;
    string value(msg_size, 'b');
    for (uint i = 0; i < maxKey; i++) {
        cout << i << " : "; 
        uint num = i;
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));
        //struct timeval t;
        //gettimeofday(&t, NULL);
        //lt1->push_back(t.tv_sec);
        //lt2->push_back(t.tv_usec);
	outstanding++;
	while (outstanding > 1000000)
	  sleep(1);

        fe->put(key, value, i);

//        if (i%15 == 0) {
//            printf("Putting! \n");
//            sleep(1);
//        }
        
        //nanosleep(&req, NULL);
        //delete key;
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

    p = true;
    string tempstr;

    cout << "Enter 'start' to start performing puts." << endl;
    cin >> tempstr;

    srand ( time(NULL) );


    double ratio = 1;
    p = true;

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

    // Randomize
    vector<uint32_t> *l  = new vector<uint32_t>;

    for (uint32_t i = 0; i < numOps; i++) {
	l->push_back(rand() % maxKey);
    }

    cout << "Enter 'start' to start performing gets." << endl;
    cin >> tempstr;

    for (uint i = 0; i < numOps; i++) {
        uint num = (*l)[i];
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        string key((char *)&key_id, sizeof(uint32_t));

	outstanding++;
	while (outstanding > 100000) {
	  printf("sleeping...\n");
	  sleep(1);
	}
        //printf("Getting key %x  (cbid %ud)\n", key_id, i);

        // Example for issuing rewrite remotely without adding a new call...
        // TODO: remove this special case when actually deploying.
//         if (i == 50000) {
//             fe->get(&key, (uint64_t)-1);
//         }

        fe->get(key, i);

        //delete key;
    }

    cout << "Done with gets..." << endl;

    return;
}


void reget_test(FrontEnd *fe, int msg_size)
{
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 200000;

    fe->register_put_cb(&put_cb);
    fe->register_get_cb(&get_cb);

    string tempstr;

    srand ( time(NULL) );

    string value(msg_size, 'b');

    // Randomize
    vector<uint32_t> *l  = new vector<uint32_t>;

    totalGets = numPutsLeft = numOps;
    maxKey = totalGets;
    for (uint32_t i = 0; i < numOps; i++) {
        uint num = rand() % maxKey;
        string ps_key((const char*)&num, sizeof(num));
        uint32_t key_id = HashUtil::BobHash((const void *)ps_key.data(), ps_key.length());
        l->push_back(key_id);
    }

    cout << "Enter 'start' to start performing tests." << endl;
    cin >> tempstr;

    for (uint32_t op = 0; op < 4; op++) {
        if (op == 0) {
            cout << "performing puts." << endl;
        } else {
            cout << "performing gets." << endl;
        }



        sem_init(&barrier, 0, 0);

        gettimeofday(&start_time, NULL);
        numGets = 0;
        for (uint i = 0; i < numOps; i++) {
            uint32_t key_id = (*l)[i];
            string key((char *)&key_id, sizeof(uint32_t));

            outstanding++;
            while (outstanding > 100000) {
                printf("sleeping...\n");
                sleep(1);
            }
            //printf("Getting key %x  (cbid %ud)\n", key_id, i);


            if (op == 0) {
                fe->put(key, value, i);
            } else {
                fe->get(key, i);
            }
        }

        cout << "Done" << endl;
        // wait on semaphore
        sem_wait(&barrier);
        cin >> tempstr;
    }

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


void delete_test(FrontEnd *fe, int msg_size)
{
    fe->register_put_cb(&put_cb2);
    fe->register_get_cb(&get_cb2);

    string tempstr;

    uint32_t key_id_1 = 1090387202;
    string key1((char *)&key_id_1, sizeof(uint32_t));

    string value(msg_size, 'b');
    cout << "Sending a put" << endl;
    fe->put(key1, value, 0);
    sleep(1);
    if (!fe->manager)
        goto error;
    
    fe->get(key1, 1);
    sleep(1);
    if (!fe->manager)
        goto error;
    
    fe->remove(key1, 2);
    sleep(1);
    if (!fe->manager)
        goto error;
    
    fe->get(key1, 3);
    sleep(1);
    cout << "done!" << endl;
    return;

error:
    fprintf(stderr, "frontend object is gone...exiting.\n");
    b_done = true;
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



    for (uint i = 0; i < maxKey; i++) {
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
    for (uint i = 0; i < maxKey; i++) {
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
    void (*testfn)(FrontEnd *, int) = join_test;
    int ch;

    while ((ch = getopt(argc, argv, "pgGbmsicden:k:")) != -1)
	switch (ch) {
	case 'p': testfn = put_test; break;
	case 'g': testfn = get_test; break;
	case 'G': testfn = reget_test; break;
        case 'b': testfn = putget_test; break;
        case 'm': testfn = maintenance_test; break;
        case 's': testfn = simple_test; break;
        case 'i': testfn = join_test; break;
        case 'd': testfn = delete_test; break;
        case 'n': numOps = atoi(optarg); break;
        case 'k': maxKey = atoi(optarg); break;
	default:
	    usage();
	    exit(-1);
	}
    argc -= optind;
    argv += optind;

    if (argc < 3) {
        usage();
        exit(-1);
    }

    string managerIP(argv[0]);
    string myIP(argv[1]);
    int myPort = atoi(argv[2]);

    int msg_size = 1024;
    if (argc >= 4) {
       msg_size  = atoi(argv[3]);
    }

    cout << "msg_size = " << msg_size << endl;
    /* Initialize ring */
    signal(SIGTERM, &sighandler);
    signal(SIGINT, &sighandler);


    sem_init(&barrier, 0, 0);

    FrontEnd *fe = new FrontEnd(managerIP, myIP, myPort);
    pthread_t localThreadId_;
    pthread_create(&localThreadId_, NULL,
                   localThreadLoop, (void*)fe);

    outstanding = 0;
    (*testfn)(fe, msg_size);

    pthread_join(localThreadId_, NULL);
    cout << "Exiting front-end manager." << endl;

    return 0;
}
