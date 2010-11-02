EXTRA_DIST = build.xml slf4j-api-1.5.8.jar slf4j-simple-1.5.8.jar libthrift.jar src

all: thrift lib tester

thrift:
	thrift -r --gen java $(top_srcdir)/fawnkv/fawnkv.thrift

lib: thrift
	ant -Dgen-ood=$(shell pwd)/gen-java -buildfile $(srcdir)/build.xml compile-ood

tester: lib
	ant -Dgen-ood=$(shell pwd)/gen-java -buildfile $(srcdir)/build.xml Tester-ood
