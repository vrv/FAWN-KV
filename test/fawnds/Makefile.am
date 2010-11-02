#hashdb_test code
noinst_PROGRAMS = fawnds_test
fawnds_test_SOURCES = fawnds_test.cc
fawnds_test_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

fawnds_test_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)
