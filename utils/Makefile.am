noinst_LTLIBRARIES = libfawnkvutils.la
noinst_HEADERS = hashutil.h dbid.h print.h timing.h fawnnet.h fnv.h debug.h
libfawnkvutils_la_SOURCES = hashutil.cc dbid.cc print.cc timing.c fawnnet.cc

# kaminsky: following line forces noinst_* libraries to build
# shared.  This can help with development because a change to
# this library doesn't require re-building libs and programs
# that link against this library
#libfawnkvutils_la_LDFLAGS = -rpath `pwd`

noinst_PROGRAMS = dbid_test
dbid_test_SOURCES = dbidtest.cc
dbid_test_CPPFLAGS = -I$(top_srcdir)/utils
dbid_test_LDADD = libfawnkvutils.la