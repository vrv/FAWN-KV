/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <sys/stat.h>
#include "node_mgr.h"
#include "dbid.h"
#include "print.h"

//#define NUM_RECORD_INIT 81943040
#define NUM_RECORD_INIT 4194304
#define NUM_RECORD_INIT_TEMP 4194304/100

void node_mgr::createIntervalFiles(const list<string>* p_vids, const list<string>* p_sids, const string my_vid)
{
    list<string>::const_iterator iter_vids, iter_sids;
    iter_sids = p_sids->begin();
    for (iter_vids = p_vids->begin(); iter_vids != p_vids->end(); iter_vids++) {
        create_interval_file(*iter_vids, *iter_sids, my_vid);
        iter_sids++;
    }
}

void node_mgr::openIntervalFiles(const list<string>* p_vids, const string my_vid)
{
    list<string>::const_iterator vid_iterator;
    int j = 0;
    for (vid_iterator = p_vids->begin(); vid_iterator != p_vids->end(); vid_iterator++) {
        interval_db* i = init_interval_db(*vid_iterator, my_vid, "");
        insertIntervalFile(i);
    }
    return;
}


void node_mgr::create_interval_file(const string ps_endid, const string ps_startid, const string my_vid) {

    string filename(filebase + bytes_to_hex(ps_endid));
    cout << "Creating file: " << filename << endl;
    cout << "[create_interval_file]  Interval is [" << bytes_to_hex(ps_startid) << " , "  << bytes_to_hex(ps_endid) << " ] " << endl;
    struct stat fileInfo;
    // truncate filename to 255 characters
    if (filename.size() > 255)
        filename.resize(255);
    int statRet = stat(filename.c_str(), &fileInfo);
    if (statRet == 0) {
        // File exists
        cout << "File exists already" << endl;
        if (overwrite)
            cout << "Overwriting..." << endl;
        else {
            cout << "Opening existing file " << endl;
            interval_db* i = init_interval_db(ps_endid, my_vid);
            insertIntervalFile(i);
            return;
        }
    }

    bool b_create_done = false;

    // lock intervaldb (read mode)
    pthread_rwlock_rdlock(&interval_lock);

    // Don't create if name already exists in existing db.
    for (uint dbi = 0; dbi < dbs.size(); dbi++) {
        if (dbs[dbi] != NULL) {
            if (dbs[dbi]->name == filename) {
                b_create_done = true;
                break;
            }
        }
    }
    // Don't hold interval lock while creating fawnds file. Assumes no
    // one else will be creating a similarly named file during this short
    // period of filename collision vulnerability
    pthread_rwlock_unlock(&interval_lock);


    if (b_create_done)
        return;

    interval_db* i = init_interval_db(ps_endid, my_vid, ps_startid);
    insertIntervalFile(i);
}


interval_db* node_mgr::init_interval_db(const string endid, const string my_vid, const string startid)
{
    string filename(filebase + bytes_to_hex(endid));
    // truncate filename to 255 characters
    if (filename.size() > 255)
        filename.resize(255);

    interval_db* i = new interval_db;
    if (startid != "") {
        cout << "[init_interval_db]  Interval is [" << bytes_to_hex(startid) << " , "  << bytes_to_hex(endid) << " ] " << endl;
        cout << "Creating file (init): " << filename << endl;
        i->h = fawn::FawnDS<FawnDS_Flash>::Create_FawnDS(filename.c_str(),
                                           NUM_RECORD_INIT,
                                           .9,
                                           .8,
                                           TEXT_KEYS);
        DBID d_endid(endid);
        DBID d_startid(startid);
        i->h->setStartID(d_startid);
        i->h->setEndID(d_endid);
    } else {
        cout << "[init_interval_db] startid isnt set" << endl;
        cout << "Opening file: " << filename << endl;
        i->h = fawn::FawnDS<FawnDS_Flash>::Open_FawnDS(filename.c_str(), TEXT_KEYS);
    }
    i->valid = true;
    i->tempDS = NULL;
    i->name = filename;
    i->splitPoint = "";
    i->largest_seq = 0;
    i->last_ack = 0;
    i->sequence = 1;

    // If vid specified, set, otherwise endid = owner vid
    if (my_vid == "")
        i->vid = endid;
    else
        i->vid = my_vid;

    pthread_rwlock_init(&(i->dbLock), NULL);
    return i;
}

// Create temporary datastore to buffer updates after precopy
// This will then be merged with the precopy datastore upon full join
// After full join, lookups will require first checking the temporarily interval version
// and if not found, the original to ensure the latest versions are retrieved.

FawnDS<FawnDS_Flash>* node_mgr::createTempStore(const string& startid_str, const string& endid_str,
                                  const string& prefix_str)
{
    DBID p_endid(endid_str);
    DBID p_startid(startid_str);
    string filename(filebase + prefix_str + bytes_to_hex(endid_str));
    // truncate filename to 255 characters
    if (filename.size() > 255)
        filename.resize(255);
    cout << "Creating temporary file: " << filename << endl;
    FawnDS<FawnDS_Flash>* tempDS = fawn::FawnDS<FawnDS_Flash>::Create_FawnDS(filename.c_str(),
                                                 NUM_RECORD_INIT_TEMP,
                                                 0.9,
                                                 0.8,
                                                 TEXT_KEYS);

    tempDS->setStartID(p_startid);
    tempDS->setEndID(p_endid);

    return tempDS;
}


/* Interval List Ops */
void node_mgr::insertIntervalFile(interval_db* i) {
    // lock interval db to atomically insert
    printf("Adding entry to interval list\n");
    pthread_rwlock_wrlock(&interval_lock);
    dbs.push_back(i);
    // unlock intervaldb
    pthread_rwlock_unlock(&interval_lock);
    return;
}

//Note: does not delete i
void node_mgr::removeIntervalFile(interval_db* i) {
    // remove intervaldb
    pthread_rwlock_wrlock(&interval_lock);
    bool found = false;
    for (uint dbi = 0; dbi < dbs.size(); dbi++) {
        if (dbs[dbi] == i) {

            dbs.erase(dbs.begin()+dbi);
            found = true;
            break;
        }
    }
    pthread_rwlock_unlock(&interval_lock);

    if (!found)
        cerr << "removeIntervalFile failed: ." << endl;

    return;
}

/**
 * Function that will return a list of all interval_db's that are
 * entirely contained in (startKey,Endkey]. Written for use by precopy
 * when the interval hasn't been merged yet.
 *
 * -jhferris, 2/21/10
 */
list<interval_db*> node_mgr::findAllIntervalDb(const string* startKey,
                                               const string* endKey,
                                               int lockDB) {
    DBID start_id(*startKey);
    DBID end_id(*endKey);

    list<interval_db*> db_list;

    // lock intervaldb
    pthread_rwlock_rdlock(&interval_lock);

    for (uint i = 0; i < dbs.size(); i++) {
        if (lockDB == WRLOCK) {
            pthread_rwlock_wrlock(&(dbs[i]->dbLock));
        } else {
            pthread_rwlock_rdlock(&(dbs[i]->dbLock));
        }

        //Unlock list so others can access
        pthread_rwlock_unlock(&interval_lock);

        // ownership => (begindID + 1) to endID  [both inclusive]

        /*
         * To explain this ugly if: we have a range (a,b] and we want
         *  all databases (c,d] such that it resides entirely within
         *  (a,b]. Checking the endpoint is easy because its
         *  inclusive, but if c == a then between will return false
         *  and it will still be a valid subrange so we check for that condition.
         */
        const DBID *d_end_id = dbs[i]->h->getEndID();
        const DBID *d_start_id = dbs[i]->h->getStartID();
        if ( (between(&start_id,&end_id, d_end_id)) &&
             (between(&start_id,&end_id, d_start_id)  ||
              (start_id == *d_start_id))
            ) {
            if (lockDB == NOLOCK) {
                pthread_rwlock_unlock(&(dbs[i]->dbLock));
            }
            db_list.push_back(dbs[i]);
        } else {
            // Release lock because you didn't find it
            pthread_rwlock_unlock(&(dbs[i]->dbLock));
        }
        delete d_end_id;
        delete d_start_id;

        // Acquire list lock again
        pthread_rwlock_rdlock(&interval_lock);
    }

    // interval_lock is locked if it gets here, so unlock
    pthread_rwlock_unlock(&interval_lock);
    return db_list;
}

interval_db* node_mgr::findIntervalDb(const string* key, int lockDB) {
    DBID p_id(*key);

    // lock intervaldb
    pthread_rwlock_rdlock(&interval_lock);

    for (uint i = 0; i < dbs.size(); i++) {
        if (lockDB == WRLOCK)
            pthread_rwlock_wrlock(&(dbs[i]->dbLock));
        else
            pthread_rwlock_rdlock(&(dbs[i]->dbLock));

        //Unlock list so others can access
        pthread_rwlock_unlock(&interval_lock);

        const DBID *d_end_id = dbs[i]->h->getEndID();
        const DBID *d_start_id = dbs[i]->h->getStartID();
        // ownership => (begindID + 1) to endID  [both inclusive]
        if (between(d_start_id, d_end_id, &p_id) && dbs[i]->valid) {
            if (lockDB == NOLOCK)
                pthread_rwlock_unlock(&(dbs[i]->dbLock));
            delete d_end_id;
            delete d_start_id;
            return dbs[i];
        }
        delete d_end_id;
        delete d_start_id;

        // Release lock because you didn't find it
        pthread_rwlock_unlock(&(dbs[i]->dbLock));

        // Acquire list lock again
        pthread_rwlock_rdlock(&interval_lock);
    }

    // interval_lock is locked if it gets here, so unlock
    pthread_rwlock_unlock(&interval_lock);
    return NULL;
}

string node_mgr::findVID(const string& key) {
    interval_db* i = findIntervalDb(&key, NOLOCK);
    assert(i != NULL);
    return i->vid;
}

void node_mgr::handle_split(const string& key) {
    /* Find interval_db that contains the split point */
    // Does not lock the db.
    interval_db* db = findIntervalDb(&key, NOLOCK);
    if (db != NULL) {
        FawnDS<FawnDS_Flash>* h = db->h;
        // Insert new interval file at the end of list
        // Current DB will always be found until we change its range.
        // Don't need lock on h to read startID because no one else should be splitting this range.
        const DBID *d_start_id = h->getStartID();
        string startRange((char*)d_start_id->const_data(), DBID_LENGTH);
        interval_db* new_interval_db = init_interval_db(key, db->vid, startRange);
        delete d_start_id;

        if (new_interval_db != NULL) {
            // update interval list
            insertIntervalFile(new_interval_db);

            cout << "Beginning split" << endl;
            stat_file << "Beginning split" << endl;
            bool success = h->Split(new_interval_db->h, &(db->dbLock));
            // h's lock is held
            if (success == true) {
                /* Update split Interval_DB with new start id */
                DBID new_start_id(key);
                db->h->setStartID(new_start_id);
                db->splitPoint = "";
                // We hold the lock on db, and we don't need to hold the lock
                // on new_interval_db because no puts have been directed to it.
                while (!db->seq_msg_queue_tmp.empty()) {
                    new_interval_db->seq_msg_queue.push_back(db->seq_msg_queue_tmp.front());
                    db->seq_msg_queue_tmp.pop_front();
                }
                // unlock h's dblock
                pthread_rwlock_unlock(&(db->dbLock));
                cout << "Ending split" << endl;
                stat_file << "Ending split" << endl;
            } else {
                cerr << "Error: split failed in splitConsumer." << endl;
            }

        } else {
            cerr << "Unable to allocate new interval file in splitConsumer." << endl;
        }

    } else {
        /* Error, hash not found */
        cerr << "Error: key does not lie in the DB ranges owned by this node!" << endl;
    }

}
