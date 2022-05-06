cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
        performance.cc
        kvstore/kvstore_api.h
        kvstore/kvstore.h
        kvstore/kvstore.cc
        kvstore/sstable.cc
        kvstore/compaction.cc
        buffer/buffer.h
        buffer/buffer.cc
        skipList/skipList.h
        skipList/skipList.cc
        memTable/container.h
        memTable/memTable.h
        memTable/memTableMap.h
        memTable/memTable.cc
        memTable/memTableMap.cc
        bloomFilter/bloomFilter.h
        bloomFilter/MurmurHash3.h
        utils/utils.h
        utils/test.h)
