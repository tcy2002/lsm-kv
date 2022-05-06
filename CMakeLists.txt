cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
        performance.cc
        cc/skipList.cc
        cc/kvstore.cc
        cc/buffer.cc
        cc/compaction.cc
        cc/sstable.cc
        cc/memTable.cc
        cc/memTableMap.cc
        header/memTableMap.h
        header/container.h
        header/memTable.h
        header/buffer.h
        header/skipList.h
        header/kvstore.h
        header/bloomFilter.h
        header/kvstore_api.h
        header/MurmurHash3.h
        header/test.h
        header/utils.h)
