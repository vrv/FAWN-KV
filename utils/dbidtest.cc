/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "dbid.h"

using namespace std;

namespace fawn {

    class DBIDUnitTest : public testing::Test {
    protected:
        // Code here will be called immediately after the constructor (right before
        // each test).
        virtual void SetUp() {

        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {

        }

        // Objects declared here can be used by all tests in the test case for HashDB.

    private:
    };

    TEST_F(DBIDUnitTest, PrintCharacterID) {
        string test = "Test Name";
        DBID testid(test);
        testid.printValue();
        testid.printValue(test.size());
    }

    TEST_F(DBIDUnitTest, CompareEqualCharacters) {
        DBID a("aaaa");
        DBID a2("aaaa");
        ASSERT_TRUE(a == a2);
    }

    TEST_F(DBIDUnitTest, CompareDifferentLengthCharacters) {
        DBID a("aaaa");
        DBID a2("aaaaa");
        ASSERT_FALSE(a == a2);
        ASSERT_TRUE(a < a2);
    }

    TEST_F(DBIDUnitTest, CompareCharacters) {
        DBID test1("aaaa");
        DBID test2("aaab");
        DBID test3("aaaB");
        ASSERT_FALSE(test1 == test2);
        ASSERT_TRUE(test1 < test2);
        ASSERT_TRUE(test3 < test1);
        ASSERT_TRUE(test3 < test2);
    }

    TEST_F(DBIDUnitTest, Between1) {
        DBID test1("aaaaaaaaaaaa");
        DBID test2("aaaaaaaaaaab");
        DBID test3("aaaaaaaaaaaB");
        // Test 3  ---  Test 1 --- Test 2

        ASSERT_TRUE(between(&test1, &test3, &test2));
        ASSERT_TRUE(between(&test2, &test1, &test3));
        ASSERT_TRUE(between(&test3, &test2, &test1));

        ASSERT_FALSE(between(&test1, &test2, &test3));
        ASSERT_FALSE(between(&test2, &test3, &test1));
        ASSERT_FALSE(between(&test3, &test1, &test2));
    }


}  // namespace fawn

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
