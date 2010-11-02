/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include "hash_functions.h"
#include "fawnds.h"
#include "fawnds_flash.h"
#include "debug.h"
#include "hashutil.h"
#include "print.h"
#include "timing.h"

using fawn::DataHeader;
using fawn::Hashes;
using fawn::HashUtil;
using fawn::DBID;

#ifndef O_NOATIME
#define O_NOATIME 0  /* O_NOATIME is linux-only */
#endif

namespace fawn {

    /***************************************************/
    /************** DB CREATION FUNCTIONS **************/
    /***************************************************/

    inline uint64_t header_size() {
        uint64_t header_size_pages = sizeof(struct DbHeader) / getpagesize();
        return (header_size_pages+1) * getpagesize();
    }

    template <typename T>
    FawnDS<T>* FawnDS<T>::Create_FawnDS(const char* filename,
                                        uint64_t hash_table_size,
                                        double max_deleted_ratio,
                                        double max_load_factor,
                                        keyType kt)
    {
        if (filename == NULL)
            return NULL;
        int fd;
        if ((fd = open(filename, O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1) {
            perror("Could not open file\n");
            return NULL;
        }

        return FawnDS<T>::Create_FawnDS_From_Fd(fd, filename,
                                                hash_table_size * FawnDS<T>::EXCESS_BUCKET_FACTOR,
                                                0, // empty
                                                0, // hashtable
                                                max_deleted_ratio,
                                                max_load_factor,
                                                kt);
    }

    template <typename T>
    int FawnDS<T>::disable_readahead() {
        int rc;
#ifdef __APPLE__
        int zero = 0;
        if ((rc = fcntl(fd_, F_RDAHEAD, &zero)) < 0)
            perror("couldn't fcntl F_RDAHEAD");
#else
        if ((rc = posix_fadvise(fd_, 0, 0, POSIX_FADV_RANDOM)) < 0)
            perror("couldn't posix_fadvise random");
#endif
        return rc;
    }


    template <typename T>
    FawnDS<T>* FawnDS<T>::Create_FawnDS_From_Fd(int fd,
                                                const string& filename,
                                                uint64_t hash_table_size,
                                                uint64_t num_entries,
                                                uint64_t deleted_entries,
                                                double max_deleted_ratio,
                                                double max_load_factor,
                                                keyType kt)
    {
        if (fd < 0)
            return NULL;

        // Calculate hashtable size based on number of entries and other hashtable parameters
        // numObjects = num_entries - deleted_entries
        // size = max(numObjects * 2, hash_table_size)
        // this means hash_table_size is a lower bound on resizing.

        uint64_t numObjects = max(hash_table_size, (num_entries - deleted_entries)*2);
        uint64_t max_entries = FindNextHashSize(numObjects);
        /* XXX - bug, it's computing this too small for non-power-of-two! */
        uint64_t db_size = header_size() + sizeof(struct HashEntry) * max_entries;

        printf("CreateFawnDS table information:\n"
               "\t hashtablesize: %"PRIu64"\n"
               "\t num_entries: %"PRIu64"\n"
               "\t deleted entries: %"PRIu64"\n"
               "\t Database header size %"PRIu64"B\n"
               "\t Total db size %"PRIu64"B\n"
               "\t Maximum number of entries: %"PRIu64"\n",
               hash_table_size, num_entries, deleted_entries,
               header_size(), db_size, max_entries);

        // extend the new file so that it can hold the header + hashtable. This
        // requires NUMPAGES(sizeof(struct DbHeader)) +
        // sizeof(struct HashEntry) * max_entries.
        if (ftruncate(fd, (off_t)db_size) == -1) {
            fprintf(stderr, "Could not extend file to %"PRIu64" bytes: %s\n",
                    db_size, strerror(errno));
        }
        lseek(fd, 0, SEEK_SET);

        if (fill_file_with_zeros(fd, db_size) < 0) {
            fprintf(stderr, "Could not zero out DB file.\n");
            return NULL;
        }

        FawnDS<T>* ds = new FawnDS<T>(filename, kt);
        ds->fd_ = fd;
        ds->datastore = new T(fd);
        ds->disable_readahead();


        // malloc header/hashtable, don't mmap
        ds->header_ = (struct DbHeader*)malloc(sizeof(struct DbHeader));
        if (ds->header_ == NULL) {
            perror("could not malloc header file\n");
            delete ds;
            return NULL;
        }
        // zero out the buffer
        memset(ds->header_, 0, sizeof(struct DbHeader));

        ds->hash_table_ = (struct HashEntry*)malloc(sizeof(struct HashEntry) * max_entries);
        if (ds->hash_table_ == NULL) {
            perror("could not malloc hash table file\n");
            delete ds;
            return NULL;
        }

        // zero out the buffer
        memset(ds->hash_table_, 0, sizeof(struct HashEntry) * max_entries);

        // REMEMBER TO WRITE OUT HEADER/HASHTABLE AFTER SPLIT/MERGE/REWRITE!

        // populate the database header.
        ds->header_->hashtable_size = max_entries;
        ds->header_->number_elements = 0;
        ds->header_->deleted_elements = 0;
        ds->header_->max_deleted_ratio = max_deleted_ratio;
        ds->header_->max_load_factor = max_load_factor;
        ds->header_->keyFormat = kt;
        ds->header_->data_insertion_point = db_size;
        ds->header_->data_start_point = db_size;

        if (lseek(ds->fd_, db_size, SEEK_SET) != (off_t)db_size) {
            fprintf(stderr, "Could not seek to offset %"PRIu64": %s\n",
                    db_size, strerror(errno));
        }

        return ds;
    }

    template <typename T>
    bool FawnDS<T>::setStartID(const DBID& sid)
    {
        memcpy(&(header_->startID), sid.const_data(), DBID_LENGTH);
        if (!WriteOnlyHeaderToFile()) {
            fprintf(stderr, "Could not set startID\n");
            return false;
        }
        return true;
    }

    template <typename T>
    bool FawnDS<T>::setEndID(const DBID& eid)
    {
        memcpy(&(header_->endID), eid.const_data(), DBID_LENGTH);
        if (!WriteOnlyHeaderToFile()) {
            fprintf(stderr, "Could not set startID\n");
            return false;
        }
        return true;
    }


    template <typename T>
    const DBID* FawnDS<T>::getStartID() const
    {
      return new DBID(header_->startID, DBID_LENGTH);
    }

    template <typename T>
    const DBID* FawnDS<T>::getEndID() const
    {
      return new DBID(header_->endID, DBID_LENGTH);
    }


    template <typename T>
    bool FawnDS<T>::WriteOnlyHeaderToFile()
    {
        uint64_t length = sizeof(struct DbHeader);
        uint64_t offset = 0;
        // write the header for the hashtable
        if ((uint64_t)pwrite64(fd_, header_, length, offset) != length) {
            fprintf(stderr, "Could not write malloc'd dbheader at position %d: %s\n", 0, strerror(errno));
            return false;
        }

        return true;
    }

    // Write out Malloc'd Header/Hashtable to Disk once DB values have been inserted sequentially
    template <typename T>
    bool FawnDS<T>::WriteHashtableToFile()
    {
        if (!WriteOnlyHeaderToFile()) {
            return false;
        }

        uint64_t offset = header_size();
        uint64_t length = sizeof(struct HashEntry) * header_->hashtable_size;

        // write the hashtable to the file
        uint64_t writeLength = length;
        ssize_t nwrite;
        while ((nwrite = pwrite64(fd_, hash_table_, writeLength, offset)) > 0) {
            writeLength -= nwrite;
            offset += nwrite;
            if (nwrite < 0) {
                perror("Error in writing");
                return false;
            }
        }

        return true;
    }

    // This assumes that the file was closed properly.
    template <typename T>
    bool FawnDS<T>::ReadHashtableFromFile()
    {
        cout << "Reading hashtable from file..." << endl;
        header_ = (struct DbHeader*)malloc(sizeof(struct DbHeader));
        if (header_ == NULL) {
            perror("could not malloc header file\n");
            return NULL;
        }

        uint64_t length = sizeof(struct DbHeader);
        uint64_t offset = 0;
        if ((uint64_t)pread64(fd_, header_, length, offset) != length) {
            fprintf(stderr, "Could not read header for data at position %"PRIu64": %s\n",
                    offset, strerror(errno));
            return false;
        }


        offset += header_size();
        length = sizeof(struct HashEntry) * header_->hashtable_size;
        hash_table_ = (struct HashEntry*)malloc(sizeof(struct HashEntry) * header_->hashtable_size);
        if (hash_table_ == NULL) {
            perror("could not malloc hash table file\n");
            free(header_);
            return NULL;
        }

        // read the hashtable from the file
        uint64_t readLength = length;
        ssize_t nread;
        uint64_t h_offset = 0;
        while ((nread = pread64(fd_, hash_table_ + h_offset, readLength, offset)) > 0) {
            readLength -= nread;
            offset += nread;
            h_offset += nread;
            if (nread < 0) {
                perror("Error in reading hashtable to file\n");
                return false;
            }
        }

        return true;
    }


    template <typename T>
    FawnDS<T>* FawnDS<T>::Open_FawnDS(const char* filename, keyType hashtable_store)
    {
        FawnDS<T>* ds = new FawnDS<T>(filename, hashtable_store);
        if (!ds->ReopenFawnDSFromFilename()) {
            delete ds;
            return NULL;
        }
        return ds;
    }

    template <typename T>
    bool FawnDS<T>::ReopenFawnDSFromFilename()
    {
        int fd;
        if ((fd = open(filename_.c_str(), O_RDWR|O_NOATIME, 0666)) == -1) {
            perror(filename_.c_str());
            return false;
        }

        struct stat statbuf;
        if (fstat(fd, &statbuf) != 0) {
            perror("could not stat file");
            return false;
        }
        if (statbuf.st_size < (int)sizeof(struct DbHeader)) {
            fprintf(stderr, "Error: database file too small\n");
            close(fd);
            return NULL;
        }

        fd_ = fd;
        datastore = new T(fd);

        if (!ReopenFawnDSFromFd()) {
            return false;
        }
        return true;
    }

    template <typename T>
    bool FawnDS<T>::ReopenFawnDSFromFd()
    {
        // will malloc in ReadHashtableFromFile
        // During rewrite, we have to free these.
        if (hash_table_)
            free(hash_table_);
        if (header_)
            free(header_);

        // Now read entries from file into memory
        if (!this->ReadHashtableFromFile()) {
            perror("could not malloc hash table file\n");
            return NULL;
        }

        disable_readahead();

        return true;
    }

    template <typename T>
    bool FawnDS<T>::deleteFile() {
        if (unlink(filename_.c_str()) == -1) {
            perror("Could not delete old database");
            return false;
        }
        return true;
    }

    /***************************************************/
    /**************** DB *STRUCTORS ********************/
    /***************************************************/

    template <typename T>
    FawnDS<T>::~FawnDS<T>()
    {
        if (hash_table_)
            free(hash_table_);
        if (header_)
            free(header_);

        if (fd_ != -1)
            close(fd_);

        delete datastore;
    }

    template <typename T>
    FawnDS<T>::FawnDS(const string& filename, keyType kt) :
        fd_(-1), header_(NULL), hash_table_(NULL),
        filename_(filename)
    {
        if (kt == RANDOM_KEYS) {
            Hashes::hashes[0] = Hashes::nullhash1;
            Hashes::hashes[1] = Hashes::nullhash2;
            Hashes::hashes[2] = Hashes::nullhash3;
        }

    }

    /***************************************************/
    /****************** DB FUNCTIONS *******************/
    /***************************************************/

    template <typename T>
    bool FawnDS<T>::Insert(const char* key, uint32_t key_len, const char* data, uint32_t length)
    {
        if (key == NULL || data == NULL)
            return false;

        if (header_->number_elements == header_->hashtable_size) {
            fprintf(stderr, "Error: hash table full!\n");
            return false;
        }

        bool newObj = false; // Used to keep track of whether this has added an object (used for tracking number of elements)
        int32_t hash_index = FindHashIndexInsert(key, key_len, &newObj);
        // If hash index is not found, we *could* rebuild the table with a larger hashtable size.
        // For now, we just return false.
        if (hash_index == -1) {
            fprintf(stdout, "Can't find index for given key in hash table.\n");
            return false;
        }

        // Do the underlying write to the datstore
        if (!datastore->Write(key, key_len, data, length, header_->data_insertion_point)) {
            fprintf(stderr, "Can't write to underlying datastore\n");
            return false;
        }

        // update the hashtable (since the data was successfully written).
        uint16_t key_in_hashtable = keyfragment_from_key(key, key_len);
        key_in_hashtable |= VALIDBITMASK; // set valid bit to 1
        hash_table_[hash_index].present_key = key_in_hashtable;
        hash_table_[hash_index].offset = header_->data_insertion_point;
        header_->data_insertion_point += sizeof(struct DataHeader) + key_len + length;

        // Update number of elements if Insert added a new object
        if (newObj)
            header_->number_elements++;

        return true;
    }

    // External deletes always write a writeLog
    template <typename T>
    bool FawnDS<T>::Delete(const char* key, uint32_t key_len)
    {
        return Delete(key, key_len, true);
    }

    template <typename T>
    bool FawnDS<T>::Delete(const char* key, uint32_t key_len, bool writeLog)
    {
        if (key == NULL)
            return false;

        DataHeader data_header;
        int32_t hash_index = FindHashIndex(key, key_len, &data_header);

        // Key is not in table
        if (hash_index == -1) {
            //fprintf(stderr, "Can't find index for given key in hash table.\n");
            return false;
        }

        // if writeLog == false, we may be ensuring that the entry was invalidated, so it's okay to check again
        if (writeLog && !valid(hash_index)) {
            fprintf(stderr, "Warning: tried to delete non-existent item\n");
            return false;
        }

        if (writeLog) {
            if (!datastore->Delete(key, key_len, header_->data_insertion_point)) {
                fprintf(stderr, "Could not delete from underlying datastore\n");
                return false;
            }
            // Update head pointer
            header_->data_insertion_point += sizeof(struct DataHeader) + key_len;

        }


        /*********   UPDATE TABLE   **********/
        hash_table_[hash_index].present_key |= DELETEDBITMASK;
        header_->deleted_elements++;

        // Eliminate stale entries on delete (exhaustive search)
        while ((hash_index = FindHashIndex(key, key_len, &data_header)) != -1) {
            hash_table_[hash_index].present_key |= DELETEDBITMASK;
        }

        return true;
    }

    template <typename T>
    bool FawnDS<T>::Get(const char* key, uint32_t key_len, string &data) const
    {
        if (key == NULL)
            return false;

        // use DataHeaderExtended for app readahead
        int32_t hash_index = 0;

        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++) {
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);
            // calculate the starting index to probe. We linearly probe
            // PROBES_BEFORE_REHASH locations before rehashing the key to get a new
            // starting index.

            // Mask lower order bits to find hash index
            uint32_t hash_index_start = indexkey & (header_->hashtable_size-1);

            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));

            for (hash_index = hash_index_start;
                 (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH;
                 ++hash_index) {
                uint16_t vkey = verifykey(hash_index);
                if (!valid(hash_index)) {
                    return false;
                }
                else if (deleted(hash_index)) {
                    continue;
                }
                else if (vkey == keyfragment_from_key(key, key_len)) {
                    off_t datapos = hash_table_[hash_index].offset;
                    if (datastore->Read(key, key_len, datapos, data))
                        return true;
                }
            }

        }
        return false;
    }


    /***************************************************/
    /************* DB REWRITE/SPLIT/MERGE **************/
    /***************************************************/

    // Rewrite needs to do the following:

    // (1) Ignore out of range items (after a database is split)
    // (2) Remove deleted items (orphans)

    // To do this:
    // Scan data region.  For every key:
    // if it falls outside the range of this database,
    //   skip it.
    // if it falls in the range of this database
    //   hash the key and do a hashtable lookup.
    //     If the entry is valid
    //       if the offset doesn't point to this item, skip this.
    //       if the offset points to this item, write it to the new database
    //     else
    //       skip it

    template <typename T>
    FawnDS<T>* FawnDS<T>::Rewrite(pthread_rwlock_t* dbLock)
    {
        // Create new database with a temporary name.
        // Should eventually pass in filebase instead of /localfs
        char *tfn = strdup("/localfs/rewrite_temp.XXXXXX");
        if (tfn == NULL) {
            fprintf(stderr, "Could not allocate memory for temporary filename.\n");
            return NULL;
        }
        int fd = mkstemp(tfn);
        const string temp_filename(tfn);
        free(tfn);

        FawnDS<T>* new_db = FawnDS<T>::Create_FawnDS_From_Fd(fd,
                                                             temp_filename,
                                                             header_->hashtable_size,
                                                             header_->number_elements,
                                                             header_->deleted_elements,
                                                             header_->max_deleted_ratio,
                                                             header_->max_load_factor,
                                                             header_->keyFormat);

        if (new_db == NULL) {
            perror("Could not create new FawnDS file.");
            return NULL;
        }

        // Transfer all key/value pairs into new database.
        const DBID* startKey = getStartID();
        const DBID* endKey = getEndID();

        off_t current_offset = header_->data_start_point;
        bool done = false;
        bool checkpointed = false;
        while (!done) {
            // atomically acquire db to check what the current data_insertion_point is
            if (dbLock) pthread_rwlock_rdlock(dbLock);

            off_t ending_offset = header_->data_insertion_point;

            if (current_offset >= ending_offset) {
                // We've caught up, the hashtable is written
                // we still hold the database lock.
                if (current_offset > ending_offset) {
                    fprintf(stderr, "This shouldn't have happened\n");
                    return NULL;
                }
                DPRINTF(DEBUG_STATUS, "Caught up.");
                done = true;
                continue;
            }

            // unlock mutex so that gets can get through.
            if (dbLock) pthread_rwlock_unlock(dbLock);

            // we know current_offset is < ending_offset, so let's process until our current knowledge of ending_offset
            while (current_offset < ending_offset) {
                DataHeader data_header;
                string key;
                if (!datastore->ReadIntoHeader(current_offset, data_header, key)) {
                    fprintf(stderr, "ReadIntoHeader failed at offset %"PRIu64".\n",
                            current_offset);
                    delete new_db;
                    if (unlink(temp_filename.c_str()) == -1) {
                        perror("Could not delete temporary file");
                    }
                    return NULL;
                }

                off_t old_offset = current_offset; // store temp
                current_offset += sizeof(struct DataHeader) + data_header.data_length + data_header.key_length;

                // if out of range, continue
                const DBID thisKey(key);
                if (!between(startKey, endKey, &thisKey)) {
                    continue;
                }

                // else, check hashtable
                int32_t hash_index = FindHashIndex(key.data(), key.length(), NULL);
                // entry is invalid (could not find key)
                if (hash_index == -1) {
                    continue;
                }

                // This check is already done in FindHashIndex.
                // entry is invalid
                if (!exists(hash_index)) {
                    continue;
                }

                // entry is invalid 3,
                // this delete log was propagated because the sender of this data
                // received a delete while the db was being scanned/sent
                // don't write the delete log, otherwise deletelogs will never disappear :)
                // write the deletelog only after the first checkpoint pass
                if (data_header.deleteLog) {
                    if (!new_db->Delete(key.data(), key.length(), checkpointed)) {
                        fprintf(stderr, "Error deleting record from other database.\n");
                        return NULL;
                    }

                }

                // if the entry doesn't point to this item...
                if (hash_table_[hash_index].offset != (off_t)old_offset) {
                    // This was an older insert
                    continue;
                }

                // else we need to copy this to the new one
                string data;
                if (!datastore->Read(key.data(), key.length(), old_offset, data)) {
                    fprintf(stderr, "Error reading from underlying datastore.\n");
                    delete new_db;
                    if (unlink(temp_filename.c_str()) == -1) {
                        perror("Could not delete temporary file");
                    }
                    return NULL;
                }

                if (!new_db->Insert(key.data(), key.length(), data.data(), data.length())) {
                    fprintf(stderr, "Error inserting to new database.\n");
                    delete new_db;
                    if (unlink(temp_filename.c_str()) == -1) {
                        perror("Could not delete temporary file");
                    }
                    return NULL;
                }
            }

            // Write out header/hashtable now
            // Only checkpoint once to avoid writing the hashtable each loop
            if (checkpointed == false) {
                if (!new_db->WriteHashtableToFile()) {
                    perror("Could not write malloc'd hashtable to fd");
                    delete new_db;
                    if (unlink(temp_filename.c_str()) == -1) {
                        perror("Could not delete temporary file");
                    }
                    return NULL;
                }
                checkpointed = true;
            }
        }

        // rewrite complete.  now need to move db over
        return new_db;
    }

    template <typename T>
    bool FawnDS<T>::RenameAndClean(FawnDS<T>* old) {
        // Delete old database.
        if (unlink(old->filename_.c_str()) == -1) {
            perror("Could not delete old database");
            return false;
        }

        // Rename new database.
        if (rename(filename_.c_str(), old->filename_.c_str()) == -1) {
            perror("Failed to rename temp database");
            return false;
        }

        // Rename local
        filename_ = old->filename_;

        // Eliminate old
        delete old;

        return true;
    }



    // These two functions allow streaming of splits.
    template <typename T>
    void FawnDS<T>::split_init(const string& destinationIP)
    {
        currSplit = header_->data_start_point;
        precopyIP = destinationIP;
    }

    // caller frees data.
    template <typename T>
    bool FawnDS<T>::split_next(const DBID* startKey, const DBID* endKey, char* ret_key, uint32_t& key_length, string& data, bool& valid, bool& remove)
    {
        remove = false;
        if (currSplit >= header_->data_insertion_point) {
            // we're done!
            return false;
        }

        DataHeader data_header;
        string key;
        if (!datastore->ReadIntoHeader(currSplit, data_header, key)) {
            fprintf(stderr, "ReadIntoHeader failed at offset %"PRIu64".\n",
                    currSplit);
            return false;
        }

        // Copy over delete log if necessary, or read into data.
        if (data_header.deleteLog) {
            remove = true;
        } else if (!datastore->Read(key.data(), key.length(), currSplit, data)) {
                fprintf(stderr, "Error reading from underlying split datastore.\n");
                return false;
        }

        currSplit += sizeof(struct DataHeader) + data_header.key_length + data_header.data_length;

        // Do range check
        const DBID thisKey(key);
        bool inRange = between(startKey, endKey, &thisKey);

        if (inRange) {
            // Return appropriate info
            valid = true;
            memcpy(ret_key, key.data(), key.length());
            key_length = key.length();
            return true;
        } else {
            cout << "CurrSplit is " << currSplit << " and inspt is " << header_->data_insertion_point;
            valid = false;
            return true;
        }
    }

    template <typename T>
    bool FawnDS<T>::Merge(FawnDS<T>* other_db, pthread_rwlock_t* dbLock)
    {
        DPRINTF(DEBUG_STATUS, "Merging...");
        // Scan other_db, insert all its keys into this.
        return other_db->SplitMergeHelper(this, dbLock, true);
    }

    template <typename T>
    bool FawnDS<T>::Split(FawnDS<T>* other_db, pthread_rwlock_t* dbLock)
    {
        DPRINTF(DEBUG_STATUS, "Splitting...");
        // Scan this db, insert specific keys into other_db
        return SplitMergeHelper(other_db, dbLock, false);
    }

    template <typename T>
    bool FawnDS<T>::SplitMergeHelper(FawnDS<T>* other_db, pthread_rwlock_t* dbLock, bool merge)
    {
        off_t current_offset = header_->data_start_point;
        bool done = false;
        const DBID* startKey = other_db->getStartID();
        const DBID* endKey = other_db->getEndID();

        bool checkpointed = false;

        while (!done) {
            // atomically acquire db to check what the current data_insertion_point is
            if (dbLock != NULL)
                pthread_rwlock_rdlock(dbLock);

            off_t ending_offset = header_->data_insertion_point;

            if (current_offset >= ending_offset) {
                // We've caught up, the hashtable is written
                // we still hold the database lock.
                if (current_offset > ending_offset) {
                    fprintf(stderr, "This shouldn't have happened\n");
                    return false;
                }
                DPRINTF(DEBUG_STATUS, "Caught up.  Need to update interval list...");
                done = true;
                continue;
            }

            // unlock mutex so that gets can get through.
            if (dbLock != NULL)
                pthread_rwlock_unlock(dbLock);

            // we know current_offset is < ending_offset, so let's process until our current knowledge of ending_offset
            while (current_offset < ending_offset) {
                DataHeader data_header;
                string key;
                if (!datastore->ReadIntoHeader(current_offset, data_header, key)) {
                    fprintf(stderr, "ReadIntoHeader failed at offset %"PRIu64".\n",
                            current_offset);
                    return false;
                }
                off_t old_offset = current_offset; // store temp
                current_offset += sizeof(struct DataHeader) + data_header.key_length + data_header.data_length;

                if (data_header.deleteLog) {
                    // otherdb must be updated if this was a delete log
                    // but delete just has to invalidate the header, no delete log or value update
                    // write the deletelog only after the first checkpoint pass
                    // compaction will ensure that this value is eventually cleaned up
                    if (!other_db->Delete(key.data(), key.length(), checkpointed)) {
                        fprintf(stderr, "Error deleting record from other database.\n");
                        return false;
                    }
                    printf("delete log...\n");
                    continue;
                }
                const DBID thisKey(key);
                bool inRange = between(startKey, endKey, &thisKey);

                // on a merge, the new db's range isn't setup yet, so let's
                // just insert everything.  if this is ever wrong,
                // compaction will clean out entries that don't belong. but
                // merging by definition means you want to insert
                // everything.

                string data;
                if (!datastore->Read(key.data(), key.length(), old_offset, data)) {
                    fprintf(stderr, "Error reading from my merging datastore.\n");
                }

                if (inRange) {
                    if (!other_db->Insert(key.data(), key.length(), data.data(), data.length())) {
                        fprintf(stderr, "Error inserting to other database.\n");
                        //return false;
                    }
                    // We no longer need to delete -- keys moved to the new
                    // database do not have to be touched here.  During
                    // compaction, we do a key-range check: since these keys
                    // will be outside the range of the DB, we can delete
                    // them then.

                } else if (merge) {
                    // must lock db to prevent concurrent insertion into "live datastore"?
                    if (!other_db->Insert(key.data(), key.length(), data.data(), data.length())) {
                        fprintf(stderr, "Error inserting to other database.\n");
                    }
                }
            }

            // current_offset = ending_offset, but ending_offset might have been updated.
            // let's write the hashtable and check again.
            // we write the hashtable here so that we don't hold the lock while the hashtable write is going on
            // otherwise we may hold the lock for this database for several seconds.

            // Only checkpoint once to avoid writing the hashtable each loop
            if (checkpointed == false) {
                DPRINTF(DEBUG_STATUS, "writing to hashtable...");
                if (!other_db->WriteHashtableToFile()) {
                    fprintf(stderr, "Could not write hashtable to file after split/merge\n");
                    return false;
                }
                checkpointed = true;
            }

        }
        // return (expect return from split to release dblock after inserting)
        return true;
    }


    /***************************************************/
    /***************** CORE FUNCTIONS ******************/
    /***************************************************/
    template <typename T>
    uint32_t FawnDS<T>::FindNextHashSize(uint32_t number)
    {
        // Gets the next highest power of 2 larger than number
        number--;
        number = (number >> 1) | number;
        number = (number >> 2) | number;
        number = (number >> 4) | number;
        number = (number >> 8) | number;
        number = (number >> 16) | number;
        number++;
        return number;
    }

    // Grab lowest order KEYFRAG bits to use as keyfragment
    // Key should be at least 2 bytes long for this to work properly
    template <typename T>
    inline uint16_t FawnDS<T>::keyfragment_from_key(const char *key, uint32_t key_len) const {
        if (key_len < sizeof(uint16_t)) {
            return (uint16_t) (key[0] & KEYFRAGMASK);
        }
        return (uint16_t) ((key[key_len-2]<<8) + key[key_len-1]) & KEYFRAGMASK;
    }

    template <typename T>
    int32_t FawnDS<T>::FindHashIndexInsert(const char* key, uint32_t key_len, bool* newObj)
    {
        int32_t hash_index = 0;
        //uint32_t searchkey = searchkey_from_key(key);
        //uint16_t hashedkey = hashkey_from_searchkey(searchkey);

        //*hashkey = hashedkey; // Save computation

        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++) {
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);
            uint32_t hash_index_start = indexkey & (header_->hashtable_size-1);
            if (hash_fn_index == 2)
                print_payload((const u_char*) &indexkey, sizeof(indexkey));

            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));

            for (hash_index = hash_index_start;
                 (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH;
                 ++hash_index) {

                uint16_t vkey = verifykey(hash_index);
                // Find first open spot or find the same key
                if (!exists(hash_index)) {
                    *newObj = true; // This is a new object, not an updated object for an existing key.
                    return hash_index;
                } else if (vkey == keyfragment_from_key(key, key_len)) {
                    off_t datapos = hash_table_[hash_index].offset;
                    DataHeader data_header;
                    string ckey;
                    if (datastore->ReadIntoHeader(datapos, data_header, ckey) &&
                        ckey.length() == key_len &&
                        memcmp(ckey.data(), key, key_len) == 0)
                        return hash_index;
                }
            }
        }
        return -1;
    }

    template <typename T>
    int32_t FawnDS<T>::FindHashIndex(const char* key, uint32_t key_len, DataHeader* data_header) const
    {
        DataHeader dummy_header;
        int32_t hash_index = 0;
        //uint32_t searchkey = searchkey_from_key(key);
        //uint16_t hashedkey = hashkey_from_searchkey(searchkey);
        //searchkey = searchkey & INDEXMASK; // pick out Index bits

        if (data_header == NULL)
            data_header = &dummy_header;

        for (u_int hash_fn_index = 0; hash_fn_index < HASH_COUNT; hash_fn_index++) {
            uint32_t indexkey = (*(Hashes::hashes[hash_fn_index]))(key, key_len);
            // calculate the starting index to probe. We linearly probe
            // PROBES_BEFORE_REHASH locations before rehashing the key to get a new
            // starting index.

            // Mask lower order bits to find hash index
            uint32_t hash_index_start = indexkey & (header_->hashtable_size-1);

            hash_index_start &= (~(PROBES_BEFORE_REHASH-1));

            for (hash_index = hash_index_start;
                 (uint32_t)hash_index < hash_index_start + PROBES_BEFORE_REHASH;
                 ++hash_index) {

                uint16_t vkey = verifykey(hash_index);

                // If empty entry, key could not be found.
                if (!valid(hash_index)) {
                    return -1;
                }
                // Skip over deleted entries
                else if (deleted(hash_index)) {
                    continue;
                }
                else if (vkey == keyfragment_from_key(key, key_len)) {
                    off_t datapos = hash_table_[hash_index].offset;
                    DataHeader data_header;
                    string ckey;
                    if (datastore->ReadIntoHeader(datapos, data_header, ckey) &&
                        ckey.length() == key_len &&
                        memcmp(ckey.data(), key, key_len) == 0)
                        return hash_index;
                }
            }

        }

        return -1;
    }

    template class FawnDS<FawnDS_Flash>;
}  // namespace fawn
