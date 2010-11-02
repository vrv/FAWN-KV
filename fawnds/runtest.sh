#!/bin/sh
make test
./test --gtest_print_time
make clean
