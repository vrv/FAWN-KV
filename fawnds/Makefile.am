noinst_LTLIBRARIES = libfawnds.la
libfawnds_la_CPPFLAGS = -I$(top_srcdir)/utils -D_FILE_OFFSET_BITS=64

noinst_HEADERS = db_structures.h fawnds.h hash_functions.h fawnds_flash.h

libfawnds_la_SOURCES =  fawnds.cc hash_functions.cc fawnds_flash.cc

# kaminsky: following line forces noinst_* libraries to build
# shared.  This can help with development because a change to
# this library doesn't require re-building libs and programs
# that link against this library
#libfawnds_la_LDFLAGS = -rpath `pwd`

noinst_PROGRAMS = fawnds_bench
fawnds_bench_SOURCES = fawnds_bench.cc
fawnds_bench_CPPFLAGS = -I$(top_srcdir)/utils
fawnds_bench_LDADD = $(top_builddir)/utils/libfawnkvutils.la libfawnds.la $(THRIFT_LIBS)

