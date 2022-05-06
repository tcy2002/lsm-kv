#pragma once

#include <map>
#include "container.h"
#include "skipList.h"

class MemTable {
private:
    uint64_t num;
    uint64_t min;
    uint64_t max;
    uint64_t mem;
    SkipList table{};

public:
    MemTable(): num(0), min(MAX_NIL), max(0), mem(BF_SIZE + 36) {}
    ~MemTable() = default;

    void put(uint64_t key, const std::string& value, bool cover=true);
    std::string get(uint64_t key);
    void scan(uint64_t key1, uint64_t key2, SkipList *list);
    Buffer *Write(std::string &path, uint64_t stamp, uint64_t dup) const;
    uint64_t getMem() const { return mem; }
    bool isEmpty() const { return (num == 0); }
    void reset();
};