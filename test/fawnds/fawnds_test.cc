/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds.h"
#include "fawnds_flash.h"
#include "config.h"
#include <string.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "print.h"
#include "hashutil.h"

namespace fawn {

    class FawnDSTest : public testing::Test {
    protected:
        // Code here will be called immediately after the constructor (right before
        // each test).
        virtual void SetUp() {
            h = FawnDS<FawnDS_Flash>::Create_FawnDS("/localfs/fawn_db", num_records_, max_deleted_ratio_,
                                                    max_load_factor_, RANDOM_KEYS);
            //h = FawnDS::Open_FawnDS("/localfs/fawn_db");

            h2 = FawnDS<FawnDS_Flash>::Create_FawnDS("/localfs/fawn_db2", num_records_, max_deleted_ratio_,
                                                     max_load_factor_, RANDOM_KEYS);
            //h2 = FawnDS::Open_FawnDS("fawn_db2");
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
            //EXPECT_EQ(0, unlink("/localfs/fawn_db"));
        }

        // Objects declared here can be used by all tests in the test case for HashDB.
        FawnDS<FawnDS_Flash> *h;
        FawnDS<FawnDS_Flash> *h2;


    private:
        static const uint64_t num_records_ = 5000000;
        static const double max_deleted_ratio_ = .9;
        static const double max_load_factor_ = .8;
    };

    TEST_F(FawnDSTest, TestSimpleInsertRetrieve) {
        const char* key = "key0";
        const char* data = "value0";
        ASSERT_TRUE(h->Insert(key, strlen(key), data, 7));

        string data_ret;
        ASSERT_TRUE(h->Get(key, strlen(key), data_ret));
        EXPECT_EQ(7, data_ret.length());
        EXPECT_STREQ(data, data_ret.data());
    }


    TEST_F(FawnDSTest, TestSimpleInsertNovalueRetrieve) {
        const char* key = "key0";
        const char* data = "";
        ASSERT_TRUE(h->Insert(key, strlen(key), data, 0));

        string data_ret;
        ASSERT_TRUE(h->Get(key, strlen(key), data_ret));
        EXPECT_EQ(0, data_ret.length());
    }

    TEST_F(FawnDSTest, TestSimpleDelete) {
        //Simple
        const char* key = "key0";
        const char* data = "value0";
        //Delete should return false
        ASSERT_FALSE(h->Delete(key, strlen(key)));
        ASSERT_TRUE(h->Insert(key, strlen(key), data, 7));

        string data_ret;
        ASSERT_TRUE(h->Get(key, strlen(key), data_ret));
        EXPECT_EQ(7, data_ret.length());
        EXPECT_STREQ(data, data_ret.data());
        // Then delete
        ASSERT_TRUE(h->Delete(key, strlen(key)));
        // Retreive should return false
        ASSERT_FALSE(h->Get(key, strlen(key), data_ret));

        const char* key2 = "key1";
        const char* data2 = "value1";
        ASSERT_TRUE(h->Insert(key2, strlen(key2), data2, 7));
        ASSERT_TRUE(h->Get(key2, strlen(key2), data_ret));
        EXPECT_EQ(7, data_ret.length());
        EXPECT_STREQ(data2, data_ret.data());
    }

    TEST_F(FawnDSTest, TestDelete) {

        char data[52];
        int* datai = (int*)data;

        for (int i = 0; i < 4; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        int i = 0;
        string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

        ASSERT_TRUE(h->Delete(key.data(), key.length()));

        // Spot open
        i = 3;
        key = HashUtil::MD5Hash((char*)&i, sizeof(i));

        for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
            datai[j] = i;
        }
        data[51] = 0;
        ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));

        // Spot filled by i=3
        key = HashUtil::MD5Hash((char*)&i, sizeof(i));
        ASSERT_TRUE(h->Delete(key.data(), key.length()));

        for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
            datai[j] = i;
        }
        data[51] = 0;
        string data_ret;
        ASSERT_FALSE(h->Get(key.data(), key.length(), data_ret));
    }


    TEST_F(FawnDSTest, Test10000InsertRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        for (int i = 0; i < 10000; ++i) {
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            string data_ret;

            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            ASSERT_TRUE(h->Get(key.data(), key.length(), data_ret));
            EXPECT_EQ(52, data_ret.length());
            EXPECT_STREQ(data, data_ret.data());
        }
    }

    TEST_F(FawnDSTest, Test10000InsertDelete) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            ASSERT_TRUE(h->Delete(key.data(), key.length()));
        }
    }

    TEST_F(FawnDSTest, Test10000InsertRewriteRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        FawnDS<FawnDS_Flash>* h_new = h->Rewrite(NULL);
        ASSERT_TRUE(h_new != NULL);
        ASSERT_TRUE(h_new->RenameAndClean(h));
        h = h_new;

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            string data_ret;
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Get(key.data(), key.length(), data_ret));
            EXPECT_EQ(52, data_ret.length());
            EXPECT_STREQ(data, data_ret.data());
        }

    }


    // fawndb should be the same size as previous fawndbs
    TEST_F(FawnDSTest, Test10000DoubleInsertRewriteRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        FawnDS<FawnDS_Flash>* h_new = h->Rewrite(NULL);
        ASSERT_TRUE(h_new != NULL);
        ASSERT_TRUE(h_new->RenameAndClean(h));
        h = h_new;

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            string data_ret;
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Get(key.data(), key.length(), data_ret));
            EXPECT_EQ(52, data_ret.length());
            EXPECT_STREQ(data, data_ret.data());
        }

    }

    TEST_F(FawnDSTest, TestWriteDB) {
        char data[1024];

        // 1GB of values
        for (int i = 0; i < 1048576; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 1024 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[1023] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 1024));
        }

        // this is required since we're not splitting/merging/rewriting initially
        ASSERT_TRUE(h->WriteHashtableToFile());

        // Temporary test of resizing hashtable
        // with current parameters it does not technically resize.
        // this was tested with numentries-deletedentries*5 in resizing calculation.
        FawnDS<FawnDS_Flash>* h_new = h->Rewrite(NULL);
        ASSERT_TRUE(h_new != NULL);
        ASSERT_TRUE(h_new->RenameAndClean(h));
        h = h_new;

    }

    TEST_F(FawnDSTest, Test10000Merge) {
        char data[52];
        for (int i = 0; i < 5000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 52));
        }

        for (int i = 5000; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h2->Insert(key.data(), key.length(), data, 52));
        }


        ASSERT_TRUE(h2->Merge(h, NULL));

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            string data_ret;
            ASSERT_TRUE(h2->Get(key.data(), key.length(), data_ret));
        }

    }

    TEST_F(FawnDSTest, Test1000000Insert) {
        char data[1000];

        for (int i = 0; i < 1000000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Insert(key.data(), key.length(), data, 1000));
        }
    }


}  // namespace fawn

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
