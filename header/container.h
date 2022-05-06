#pragma once

#include <iostream>
#include "buffer.h"
#include "skipList.h"

class container {
protected:
    uint64_t num;
    uint64_t min;
    uint64_t max;
    uint64_t mem;

public:
    container(): num(0), min(INT64_MAX), max(0), mem(BF_SIZE + 36) {}
    ~container() = default;

    virtual void put(uint64_t key, const std::string& value, bool cover) = 0;
    virtual std::string get(uint64_t key) = 0;
    virtual void scan(uint64_t key1, uint64_t key2, SkipList *list) = 0;
    virtual Buffer *Write(std::string &path, uint64_t stamp, uint64_t dup) const = 0;

    uint64_t getMem() const { return mem; }
    bool isEmpty() const { return (num == 0); }
    void resetData() {
        num = 0;
        min = INT64_MAX;
        max = 0;
        mem = BF_SIZE + 36;
    }
};