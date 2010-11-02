/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>
#include <assert.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include "hash_functions.h"
#include "fawnds_flash.h"
#include "debug.h"
#include "hashutil.h"
#include "print.h"
#include "timing.h"

using fawn::DataHeader;
using fawn::Hashes;
using fawn::HashUtil;

#ifndef O_NOATIME
#define O_NOATIME 0  /* O_NOATIME is linux-only */
#endif

namespace fawn {
    /***************************************************/
    /****************** DB FUNCTIONS *******************/
    /***************************************************/

    bool FawnDS_Flash::Write(const char* key, uint32_t key_len, const char* data, uint32_t length, off_t offset)
    {
        struct DataHeader data_header;
        data_header.data_length = length;
        data_header.key_length = key_len;
        data_header.deleteLog = false;

        struct iovec iov[3];
        iov[0].iov_base = &data_header;
        iov[0].iov_len = sizeof(struct DataHeader);
        iov[1].iov_base = const_cast<char *>(key);
        iov[1].iov_len = key_len;
        iov[2].iov_base = const_cast<char *>(data);
        iov[2].iov_len = length;

        if (lseek(fd_, offset, SEEK_SET) != offset) {
            fprintf(stderr, "Could not seek to offset %"PRIu64": %s\n",
                    offset, strerror(errno));
        }

        if (writev(fd_, iov, 3) != (ssize_t) (sizeof(struct DataHeader) + key_len + length)) {
            fprintf(stderr, "Could not write iovec structure: %s\n", strerror(errno));
            return false;
        }

        return true;
    }

    bool FawnDS_Flash::Delete(const char* key, uint32_t key_len, off_t offset)
    {
        /*********   DELETE LOG    **********/
        // Just needs the key and the fact that it's deleted to be appended.
        struct DataHeader delete_header;
        delete_header.data_length = 0;
        delete_header.key_length = key_len;
        delete_header.deleteLog = true;

        if ((uint64_t)pwrite64(fd_, &delete_header, sizeof(struct DataHeader),
                               offset) != sizeof(struct DataHeader)) {
            fprintf(stderr, "Could not write delete header at position %"PRIu64": %s\n",
                    (uint64_t)offset, strerror(errno));
            return false;
        }

        if ((uint64_t)pwrite64(fd_, key, key_len,
                               offset + sizeof(struct DataHeader)) != key_len) {
            fprintf(stderr, "Could not write delete header at position %"PRIu64": %s\n",
                    (uint64_t)offset + sizeof(struct DataHeader), strerror(errno));
            return false;
        }

        return true;
    }

    bool FawnDS_Flash::ReadIntoHeader(off_t offset, DataHeader &data_header, string &key)
    {
        ssize_t n_read = pread64(fd_, &data_header, sizeof(struct DataHeader), offset);
        if (n_read < (ssize_t)sizeof(struct DataHeader)) {
            fprintf(stderr, "Read %lu bytes from DataHeader, expected %lu\n", n_read, sizeof(struct DataHeader));
            return false;
        }
        uint32_t key_len = data_header.key_length;
        char *mdata = (char *)malloc(key_len);

        n_read = pread64(fd_, mdata, key_len, offset + sizeof(struct DataHeader));
        if (n_read < key_len) {
            fprintf(stderr, "Read %lu bytes from key, expected %u\n", n_read, key_len);
            return false;
        }
        key.assign(mdata, key_len);
        free(mdata);
        return true;
    }

    bool FawnDS_Flash::Read(const char* key,
                            uint32_t key_len,
                            off_t offset,
                            string &data)
    {
        DataHeader data_header;
        string inputkey;

        if (!ReadIntoHeader(offset, data_header, inputkey)) {
            return false;
        }

        // Hashing based on key fragment can result in potential key collision
        // So we read the dataheader here to compare the full key to ensure this.
        if (memcmp(inputkey.data(), key, key_len) != 0) {
            return false;
        }

        size_t length = data_header.data_length;
        if (length == 0) {
            return true;
        }

#if 0
        // Readahead code -- n_read was removed because it was put into ReadIntoHeader
        // For readahead, you have to re-introduce that code into this function
        if (length < (n_read - sizeof(struct DataHeader))) {
            //printf("GDOEX skipped pread\n");
            data.assign(data_header.partial_data, length);
        } else
#endif
        {
            char *mdata = (char *)malloc(length);
            //printf("GDOEX pread64: %x  (%d)\n", datapos + sizeof(DataHeader), length);
            if ((uint64_t)pread64(fd_, mdata, length, offset + key_len + sizeof(struct DataHeader)) !=
                length) {
                fprintf(stderr, "Could not read data at position %"PRIu64": %s\n",
                        offset + sizeof(DataHeader), strerror(errno));
                free(mdata);
                return false;
            }
            data.assign(mdata, length);
            free(mdata);
            /* SPEED note:  May be worth some day eliminating the redundant
             * data copy in this by figuring out how to read directly into the
             * string's buffer.  Only matters for values > 2k where we
             * can't do readahead. */
            /* Better speed note:  Fix them *both*.  Don't use a string, use
             * our own thing, and do all of the preads() directly into it.
             * Treat it like an mbuf/skbuf, and be able to advance the
             * "start" pointer into the buffer so we never have to copy
             * regardless of how we get the data.  */
        }
        return true;
    }




}  // namespace fawn
