/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_H_
#define _FAWNDS_H_

#include <sys/types.h>
#include <string>
#include <stdint.h>

#include "hash_functions.h"
#include "db_structures.h"
#include "dbid.h"
#include <pthread.h>

#ifdef __APPLE__
#define pread64 pread
#define pwrite64 pwrite
#endif // #ifdef __APPLE__

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_source
#endif  // #ifndef _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

using namespace std;

namespace fawn {

    template <typename T>
    class FawnDS {
    public:
        static FawnDS<T>* Create_FawnDS(const char* filename, uint64_t hash_table_size,
                                        double max_deleted_ratio, double max_load_factor,
                                        keyType kt = TEXT_KEYS)
            __attribute__ ((warn_unused_result));

        static FawnDS<T>* Open_FawnDS(const char* filename, keyType kt = TEXT_KEYS);

        const DBID* getStartID() const;
        const DBID* getEndID() const;
        bool    setStartID(const DBID& sid);
        bool    setEndID(const DBID& eid);

        virtual ~FawnDS();

        bool Insert(const char* key, uint32_t key_len, const char* data, uint32_t length)
            __attribute__ ((warn_unused_result));
        bool Delete(const char* key, uint32_t key_len) __attribute__ ((warn_unused_result));
        bool Delete(const char* key, uint32_t key_len, bool writeLog) __attribute__ ((warn_unused_result));
        bool Get(const char* key, uint32_t key_len, string &data) const
            __attribute__ ((warn_unused_result));

//         bool Insert(const char* key, const char* data, uint32_t length)
//             __attribute__ ((warn_unused_result));
//         bool Delete(const char* key) __attribute__ ((warn_unused_result));
//         bool Delete(const char* key, bool writeLog) __attribute__ ((warn_unused_result));
//         bool Get(const char* key, string &data) const
//             __attribute__ ((warn_unused_result));

        FawnDS<T>* Rewrite(pthread_rwlock_t* dbLock) __attribute__ ((warn_unused_result));
        bool RenameAndClean(FawnDS<T>* old) __attribute__ ((warn_unused_result));
        bool Merge(FawnDS<T>* other_db, pthread_rwlock_t* listLock) __attribute__ ((warn_unused_result));
        bool Split(FawnDS<T>* other_db, pthread_rwlock_t* listLock)
            __attribute__ ((warn_unused_result));

        void split_init(const string& destinationIP);
        bool split_next(const DBID* startKey, const DBID* endKey, char* ret_key, uint32_t& key_length, string& data, bool& valid, bool& remove);


        bool SplitMergeHelper(FawnDS<T>* other_db, pthread_rwlock_t* listLock, bool merge)
            __attribute__ ((warn_unused_result));

        // Used to write header/hashtable sequentially for split/merge/rewrite
        bool WriteHashtableToFile() __attribute__ ((warn_unused_result));

        // Used to write header/hashtable sequentially for split/merge/rewrite
        bool WriteOnlyHeaderToFile() __attribute__ ((warn_unused_result));

        // Used to read header/hashtable sequentially when header is malloc'd
        bool ReadHashtableFromFile() __attribute__ ((warn_unused_result));


        //inline string getPrecopyIP() { return precopyIP; }

        // per db lock
        string precopyIP;

        bool deleteFile();
    private:
        FawnDS<T>(const string& filename, keyType storeType);
        inline uint16_t keyfragment_from_key(const char* key, uint32_t key_len) const;

        // Locate the hashtable entry that this key occupies or would occupy. Methods
        // that use this utility function can check if that entry exists or not by
        // checking the valid flag. Returns -1 on failure.
        int32_t FindHashIndexInsert(const char* key, uint32_t key_len, bool* newObj);
        int32_t FindHashIndex(const char* key, uint32_t key_len, DataHeader* data_header) const;

        // This function is used to populate the member variables of this object with
        // a different file.
        bool ReopenFawnDSFromFd();
        bool ReopenFawnDSFromFilename();

        // This function returns a FawnDS object that does not have its filename
        // object variable set. This variable is important for functions such as
        // Rewrite(), Merge() and Split() so that we can delete the original file and
        // overwrite it with the new file. Therefore, it is the caller's
        // responsibility to either fill in that field or to never use the Rewrite(),
        // Merge() or Split() methods on the returned object.

        // Helper function to malloc or mmap header/hashtable
        static FawnDS<T>* Create_FawnDS_From_Fd(int fd,
                                                const string& filename,
                                                uint64_t hash_table_size,
                                                uint64_t number_entries,
                                                uint64_t deleted_entries,
                                                double max_deleted_ratio,
                                                double max_load_factor,
                                                keyType kt)
            __attribute__ ((warn_unused_result));

        static uint32_t FindNextHashSize(uint32_t number);

        // Incrementing keyfragbits above 15 requires
        // more modifications to code (e.g. hashkey is 16 bits in (Insert())
        static const uint32_t KEYFRAGBITS = 14;
        static const uint32_t KEYFRAGMASK = (1 << KEYFRAGBITS) - 1;
        static const uint32_t DELETEDBITMASK = (2 << KEYFRAGBITS);
        static const uint32_t INDEXBITS = 16;
        static const uint32_t INDEXMASK = (1 << INDEXBITS) - 1;
        static const uint32_t VALIDBITMASK = KEYFRAGMASK+1;

        // Check if highest bit is 1; means deleted.
        inline bool deleted(int32_t index) const
        {
            return (hash_table_[index].present_key & DELETEDBITMASK);
        }

        inline bool valid(int32_t index) const
        {
            return (hash_table_[index].present_key & VALIDBITMASK);
        }

        // Used by Insert: check that entry is not deleted or empty
        inline bool exists(int32_t index) const
        {
            return valid(index) && !deleted(index);
        }

        inline uint16_t verifykey(int32_t index) const
        {
            return (hash_table_[index].present_key & KEYFRAGMASK);
        }

        int disable_readahead();

        int fd_;
        struct DbHeader* header_;
        struct HashEntry* hash_table_;
        T* datastore;
        string filename_;
        off_t currSplit;

        static const double EXCESS_BUCKET_FACTOR = 1.1;
    };

}  // namespace fawn

#endif  // #ifndef _FAWNDS_H_
