noinst_LTLIBRARIES = libfawnkv.la
libfawnkv_la_CPPFLAGS = -I$(top_srcdir)/utils  		\
			-I$(builddir)/gen-cpp		\
		        -I$(top_srcdir)/fawnds		\
		        -I$(top_builddir)/fawnds

nodist_libfawnkv_la_SOURCES =				\
	$(builddir)/gen-cpp/FawnKVBackend.cpp 		\
	$(builddir)/gen-cpp/FawnKVBackend.h 		\
	$(builddir)/gen-cpp/FawnKVFrontend.cpp 		\
	$(builddir)/gen-cpp/FawnKVFrontend.h 		\
	$(builddir)/gen-cpp/FawnKVManager.cpp 		\
	$(builddir)/gen-cpp/FawnKVManager.h 		\
	$(builddir)/gen-cpp/FawnKV.cpp 			\
	$(builddir)/gen-cpp/FawnKV.h 			\
	$(builddir)/gen-cpp/FawnKVApp.cpp 		\
	$(builddir)/gen-cpp/FawnKVApp.h 		\
	$(builddir)/gen-cpp/fawnkvmesg_constants.cpp	\
	$(builddir)/gen-cpp/fawnkvmesg_constants.h	\
	$(builddir)/gen-cpp/fawnkvmesg_types.cpp 	\
	$(builddir)/gen-cpp/fawnkvmesg_types.h 		\
	$(builddir)/gen-cpp/fawnkv_constants.cpp 	\
	$(builddir)/gen-cpp/fawnkv_constants.h 		\
	$(builddir)/gen-cpp/fawnkv_types.cpp 		\
	$(builddir)/gen-cpp/fawnkv_types.h

SKELETON_CODE = 						\
	$(builddir)/gen-cpp/FawnKVApp_server.skeleton.cpp	\
	$(builddir)/gen-cpp/FawnKVBackend_server.skeleton.cpp	\
	$(builddir)/gen-cpp/FawnKVFrontend_server.skeleton.cpp	\
	$(builddir)/gen-cpp/FawnKVManager_server.skeleton.cpp	\
	$(builddir)/gen-cpp/FawnKV_server.skeleton.cpp

BUILT_SOURCES = $(nodist_libfawnkv_la_SOURCES)

$(nodist_libfawnkv_la_SOURCES) : fawnkv.thrift fawnkvmesg.thrift
	thrift -r -o $(builddir) --gen cpp $(srcdir)/fawnkvmesg.thrift
	thrift -r -o $(builddir) --gen cpp $(srcdir)/fawnkv.thrift

libfawnkv_la_SOURCES = 				\
	fawnkvmesg.thrift 			\
	fawnkv.thrift				\
	node.cpp				\
	node.h					\
	nodehandle.cpp 				\
	nodehandle.h 				\
	ring.cpp				\
	ring.h					\
	fe.cpp					\
	fe.h					\
	virtualnode.cpp				\
	virtualnode.h

# kaminsky: following line forces noinst_* libraries to build
# shared.  This can help with development because a change to
# this library doesn't require re-building libs and programs
# that link against this library
#libfawnkv_la_LDFLAGS = -rpath `pwd`

noinst_PROGRAMS = manager backend frontend

manager_SOURCES = Manager.cpp Manager.h

manager_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(srcdir)	 			\
	-I$(builddir)/gen-cpp		 	\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnkv 		\
	-I$(top_builddir)/fawnds

manager_LDADD = 				\
	$(top_builddir)/fawnkv/libfawnkv.la	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)


backend_CPPFLAGS = -I$(top_srcdir)/utils        \
		   -I$(top_srcdir)/fawnds	\
		   -I$(top_builddir)/fawnds	\
		   -I$(srcdir)			\
		   -I$(builddir)/gen-cpp

backend_SOURCES = node_mgr_db.cpp node_mgr.cpp node_mgr.h FawnKVBackendHandler.cpp

backend_LDADD = 				\
    $(top_builddir)/fawnds/libfawnds.la 	\
    $(top_builddir)/utils/libfawnkvutils.la	\
    libfawnkv.la				\
    $(THRIFT_LIBS)


frontend_SOURCES = FawnKVServerHandler.cpp FawnKVServerHandler.h fe_cache.cpp fe_cache.h

frontend_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(srcdir)				\
	-I$(builddir)/gen-cpp	 		\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnkv 		\
	-I$(top_builddir)/fawnds

frontend_LDADD = 				\
	$(top_builddir)/fawnkv/libfawnkv.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

CLEANFILES = $(nodist_libfawnkv_la_SOURCES) $(SKELETON_CODE)
