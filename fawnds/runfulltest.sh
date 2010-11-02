#!/bin/sh
mv fawnds_test.cc fawnds_test.small
mv fawnds_test.full fawnds_test.cc
make test
./test --gtest_print_time
make clean
mv fawnds_test.cc fawnds_test.full
mv fawnds_test.small fawnds_test.cc
