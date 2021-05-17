SHENANGO_PATH=../../shenango
include $(SHENANGO_PATH)/shared.mk

librt_libs = $(SHENANGO_PATH)/bindings/cc/librt++.a
INC += -I$(SHENANGO_PATH)/bindings/cc -I$(SHENANGO_PATH)/ksched -I../inc \
       -I../DataFrame/AIFM/include/

array_rw_test_src = array_rw_test.cpp
array_rw_test_obj = $(array_rw_test_src:.cpp=.o)

lib_src = $(wildcard ../src/*.cpp)
lib_src := $(filter-out ../src/tcp_device_server.cpp,$(lib_src))
lib_obj = $(lib_src:.cpp=.o)

src = $(lib_src) $(array_rw_test_src)
obj = $(src:.cpp=.o)
dep = $(obj:.o=.d)

override CXXFLAGS += -std=gnu++2a -fconcepts -Wno-unused-function
CXXFLAGS := $(filter-out -std=gnu++17,$(CXXFLAGS))
override LDFLAGS += -lnuma


all: bin/array_rw_test

bin/array_rw_test: $(array_rw_test_obj) $(librt_libs) $(RUNTIME_DEPS) $(lib_obj)
	$(LDXX) -o $@ $(array_rw_test_obj) $(lib_obj) $(librt_libs) $(RUNTIME_LIBS) $(LDFLAGS)


#rule to generate a dep file by using the C preprocessor
#(see man cpp for details on the - MM and - MT options)
%.d: %.cpp
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)   # include all dep files in the makefile
endif

.PHONY: clean
clean:
	rm -f src/*.o test/*.o $(dep) bin/* lib*.a
