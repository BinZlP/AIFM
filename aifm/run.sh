#!/bin/bash

log_folder=`pwd`
log_folder=$log_folder/myexp

source shared.sh

cache_sizes=(1 3 5 7 9 11 13 15 17 19 21)

sudo pkill -9 main

CXXFLAGS="-DDISABLE_OFFLOAD_UNIQUE -DDISABLE_OFFLOAD_COPY_DATA_BY_IDX -DDISABLE_OFFLOAD_SHUFFLE_DATA_BY_IDX -DDISABLE_OFFLOAD_ASSIGN -DDISABLE_OFFLOAD_AGGREGATE"
#make clean
#make -j CXXFLAGS="$CXXFLAGS"

#cd myexp
for cache_size in ${cache_sizes[@]}
do
    echo "Cache size: $cache_size"
    sed "s/constexpr uint64_t kCacheGBs.*/constexpr uint64_t kCacheGBs = $cache_size;/g" test/array_rw_test.cpp -i
    make clean
    make -j CXXFLAGS="$CXXFLAGS"
    rerun_local_iokerneld_noht
    rerun_mem_server
    run_program_noht ./bin/array_rw_test 1>$log_folder/log.$cache_size 2>&1
    echo
    echo
done
kill_local_iokerneld
