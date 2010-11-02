/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _DB_STRUCTURES_H_
#define _DB_STRUCTURES_H_

#include <stdint.h>
#include "dbid.h"

#define PROBES_BEFORE_REHASH 8


namespace fawn {

    enum keyType { TEXT_KEYS, RANDOM_KEYS };

    struct DbHeader {
        uint64_t magic_number;
        uint64_t hashtable_size;
        uint64_t number_elements;
        uint64_t deleted_elements;
        double max_deleted_ratio;
        double max_load_factor;
        keyType keyFormat;
        off_t data_insertion_point;  // offset to where the next record should go
        off_t data_start_point;  // offset showing where first record is
        char startID[DBID_LENGTH];
        char endID[DBID_LENGTH];
    } __attribute__((__packed__));


    /*
      Hash Entry Format
      D = Is slot deleted: 1 means deleted, 0 means not deleted.  Needed for lazy deletion
      V = Is slot empty: 0 means empty, 1 means taken
      K = Key fragment
      O = Offset bits
      ________________________________________________
      |DVKKKKKKKKKKKKKKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO|
      ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
    */
    struct HashEntry {
        uint16_t present_key;
        uint32_t offset;
    } __attribute__((__packed__));

    struct DataHeader {
        uint32_t data_length;
        uint32_t key_length;
        bool deleteLog;
    } __attribute__((__packed__));

    static const int DSReadMin = 2048;
    struct DataHeaderExtended {
        uint32_t data_length;
        uint32_t key_length;
        bool deleteLog;
        char partial_data[DSReadMin-sizeof(struct DataHeader)];
    } __attribute__((__packed__));

}  // namespace fawn

#endif  // #define _DB_STRUCTURES_H_
