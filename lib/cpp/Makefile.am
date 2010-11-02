BUILT_SOURCES =	gen-cpp/FawnKV.cpp gen-cpp/FawnKV.h			\
		gen-cpp/fawnkv_constants.cpp gen-cpp/fawnkv_constants.h	\
		gen-cpp/fawnkv_types.cpp gen-cpp/fawnkv_types.h

# This could be cleaned up but I don't know the best way
gen-cpp/FawnKV.cpp: $(top_srcdir)/fawnkv/fawnkv.thrift
gen-cpp/FawnKV.h: $(top_srcdir)/fawnkv/fawnkv.thrift
gen-cpp/fawnkv_constants.cpp: $(top_srcdir)/fawnkv/fawnkv.thrift
gen-cpp/fawnkv_constants.h: $(top_srcdir)/fawnkv/fawnkv.thrift
gen-cpp/fawnkv_types.cpp: $(top_srcdir)/fawnkv/fawnkv.thrift
gen-cpp/fawnkv_types.h: $(top_srcdir)/fawnkv/fawnkv.thrift

noinst_PROGRAMS = tester tester_remote

tester_SOURCES = tester.cpp TFawnKV.cpp TFawnKV.h

tester_CPPFLAGS = -I$(builddir)/gen-cpp -I$(top_srcdir)/utils/ -I$(top_srcdir)/fawnkv/ -I$(top_builddir)/fawnkv/gen-cpp/

tester_LDADD = 	 				\
	$(top_builddir)/fawnkv/libfawnkv.la	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

tester_remote_SOURCES = tester_remote.cpp TFawnKVRemote.cpp TFawnKVRemote.h

tester_remote_CPPFLAGS = -I$(builddir)/gen-cpp -I$(top_srcdir)/utils/ -I$(top_srcdir)/fawnkv/ -I$(top_builddir)/fawnkv/gen-cpp/

tester_remote_LDADD = 	 			\
	$(top_builddir)/fawnkv/libfawnkv.la	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)


CLEANFILES = $(BUILT_SOURCES)
