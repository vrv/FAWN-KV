/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* Cache tester by Jason Franklin (jfrankli@cs.cmu.edu) */
/* Derived from fe_tester.cpp */
using namespace std;

#include <iostream>
#include <sstream>
#include <string>
#include "fe.h"
#include "dbid.h"
#include "hashutil.h"
#include "timing.h"

using fawn::HashUtil;
using fawn::DBID;

#include <unistd.h>
#include <map>
#include <list>
#include <pthread.h>

int total = 0;

int numGets = 0;
struct timeval start;

string* value;
bool printedStats;

void put_cb(unsigned int continuation)
{
#ifdef DEBUG
  cout << "Woah! Got to put_cb." << endl;
#endif

}

void get_cb(DBID* p_key, string* p_value, unsigned int continuation, bool success, bool cached)
{
#ifdef DEBUG
  cout << "Woah! Got to get_cb. " << endl;
#endif
#ifdef DEBUG
    cout << "\t Key: " << p_key->actual_data_str() << endl;

    if (p_value) {
        cout << "\t Value: " << *p_value << endl;
    }
    else {
        cout << "\t Value: NULL" << endl;
    }

    cout << "\t Continuation Id: " << continuation << endl;

    cout << "\t Get Success?: " << (success ? "yes!" : "no") << " Success #: " ;
#endif

    if (p_value->compare(value->c_str())) {
      cout << "ERROR: returned value does not match expected value." << endl;
      cout << "value is: " << *p_value << endl;
    }

    numGets++;

    if (numGets % 100000 == 0)
      cout << "Number of successful gets: " << numGets << endl;

    if (((double)numGets) >= ((double)(0.9)*total) && !printedStats) {

	struct timeval end;
	gettimeofday(&end, NULL);
	double diff = timeval_diff(&start, &end);
	cout << "Num Queries: " << total << " time: " << diff << endl;
	cout << "Query Rate: " << (total / diff) << " qps" << endl;
	printedStats = true;
    }

    if (success) {
	//if (numGets % 100 == 0)
	//cout << "Successful gets: " << ++numGets << endl;

    } else {
      cout << "Fail" << endl;
      cout << endl; // flush!
    }

    if (!cached && p_key)
      delete p_key;
    if (!cached && p_value) {
      delete p_value;
    }

    //cout << "exiting get cb" << endl;
}


void *localThreadLoop(void *p)
{
    FrontEnd* fe = (FrontEnd*) p;

    // TODO: Fix Hack. Just used to keep client around.
    while(1) {
        fe->log_stats();
	sleep(1);
    }
    return NULL;
}

void usage()
{
    cerr << "./fe_tester <My IP> <Corpus Size> <Bias Size> <Num Gets> <Cache Entries>\n" <<endl;
}

int main(int argc, char **argv)
{

  if (argc != 6) {
    usage();
    exit(-1);
  }

  string p_myIP(argv[1]);
  int corpus = atoi(argv[2]);
  int bias = atoi(argv[3]);
  int num_gets = atoi(argv[4]);
  int num_cache_entries = atoi(argv[5]); /* N log N entries, where N = num wimpies */

  cout << "Corpus: " << corpus << endl
       << " Bias: " << bias << endl
       << " Queries: " << num_gets << endl
       << " Cache Entries: " << num_cache_entries << endl;

  total = num_gets;

  /* Initialize ring with cache enabled */
  FrontEnd* p_fe = FrontEnd::Instance(p_myIP, true, num_cache_entries);

  value = new string(256, 'v');

  printedStats = false;

   pthread_t localThreadId_;
   pthread_create(&localThreadId_, NULL, 
 		 localThreadLoop, (void*)p_fe);

  p_fe->register_put_cb(&put_cb);
  p_fe->register_get_cb(&get_cb);
  srand ( time(NULL) );
  struct timespec req;
  req.tv_sec = 0;
  req.tv_nsec = 50000;

  cout << "Enter 'start' to start performing puts." << endl;
  string tempstr;
  cin >> tempstr;

  /* Load database */
  for (int i = 0; i < corpus; i++) {
    ostringstream s;
    s << "key" << i;
    string ps_key = s.str();
    
    if (i % 1000000 == 0)
      cout << "inserting: " << ps_key << endl;

    u_int32_t key_id = HashUtil::MakeHashedKey(ps_key);
    DBID* key = new DBID((char *)&key_id, sizeof(u_int32_t));
    p_fe->put(key, value, i);
    delete key;
    
    nanosleep(&req, NULL);
  }

  cout << "Enter 'start' to start performing gets." << endl;
  cin >> tempstr;

  /* Fetch items at random */
  int num = 0;
  
  gettimeofday(&start, NULL);
  for (int i = 0; i < num_gets; i++) {

    /* Biasing */
    int rand_val = rand() % 100;
    double prob_pick_hot_item = ((double) (corpus - bias)) / corpus;
    int perc_pick_hot_item = (int) (prob_pick_hot_item * ((double) 100));

    //cout << "Random value (between 0 and 100): " << rand_val << endl;
    //cout << "Probability of picking hot item: " << prob_pick_hot_item << endl;
    //cout << "Percentage of picking hot item: " << perc_pick_hot_item << endl;

    if (rand_val <= perc_pick_hot_item) {
      num = rand()% bias;
      //cout << "Fetching hot item " << num << endl;
    } else {
      //cout << "Cold item fetch: " << num << endl;
      num = rand()%corpus;
      if (num <= bias) {
	num = ((int)((corpus-bias)*(rand_val/100))) + bias;
      }

    }

    ostringstream out2;
    out2 << "key" << num;
    string ps_key = out2.str();

    u_int32_t key_id = HashUtil::MakeHashedKey(ps_key);

    DBID* key_p = new DBID((char *)&key_id, sizeof(u_int32_t));

    if (key_p == NULL) {
      cout << "ERROR: Malloc of ID failed. Out of Memory." << endl;
    } /* XXX - and yet, we then pass it to get? */
  
    p_fe->get(key_p, 2);
    
    delete key_p;
  }

  pthread_join(localThreadId_, NULL);
    cout << "Exiting front-end manager." << endl;

    FrontEnd::Destroy();
    return 0;

}
