BUILT_SOURCES =	gen-rb/fawn_k_v.rb \
		gen-rb/fawn_k_v_app.rb \
		gen-rb/fawnkv_constants.rb \
		gen-rb/fawnkv_types.rb

# This could be cleaned up but I don't know the best way
gen-rb/fawn_k_v.rb: $(top_srcdir)/fawnkv/fawnkv.thrift
	thrift -r --gen rb $(top_srcdir)/fawnkv/fawnkv.thrift
gen-rb/fawn_k_v_app.rb: $(top_srcdir)/fawnkv/fawnkv.thrift
	thrift -r --gen rb $(top_srcdir)/fawnkv/fawnkv.thrift
gen-rb/fawnkv_constants.rb: $(top_srcdir)/fawnkv/fawnkv.thrift
	thrift -r --gen rb $(top_srcdir)/fawnkv/fawnkv.thrift
gen-rb/fawnkv_types.rb: $(top_srcdir)/fawnkv/fawnkv.thrift
	thrift -r --gen rb $(top_srcdir)/fawnkv/fawnkv.thrift


CLEANFILES = $(BUILT_SOURCES)
