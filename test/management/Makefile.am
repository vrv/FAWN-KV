#ringtester code
noinst_PROGRAMS = ringtester lattester
ringtester_SOURCES = ringtester.cpp
ringtester_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnkv 			\
	-I$(top_builddir)/fawnkv/gen-cpp 		\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnkv 		\
	-I$(top_builddir)/fawnds

ringtester_LDADD = 				\
	$(top_builddir)/fawnkv/libfawnkv.la	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

lattester_SOURCES = lattester.cpp
lattester_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnkv 			\
	-I$(top_builddir)/fawnkv/gen-cpp 		\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnkv 		\
	-I$(top_builddir)/fawnds

lattester_LDADD = 				\
	$(top_builddir)/fawnkv/libfawnkv.la	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)
