/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_FLASH_H_
#define _FAWNDS_FLASH_H_

#include <sys/types.h>
#include <string>
#include <stdint.h>
#include "db_structures.h"

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

    class FawnDS_Flash {
    public:
        FawnDS_Flash(int fd) : fd_(fd) {}
        ~FawnDS_Flash() {}
        bool Write(const char* key, uint32_t key_len, const char* data, uint32_t length, off_t offset);
        bool Delete(const char* key, uint32_t key_len, off_t offset);
        bool ReadIntoHeader(off_t offset, DataHeader &data_header, string &key);// const;
        bool Read(const char* key, uint32_t key_len, off_t offset, string &data);// const;
    private:
        int fd_;
    };

}  // namespace fawn

#endif  // #ifndef _FAWNDS_FLASH_H_
