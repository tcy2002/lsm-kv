#pragma once

#include <map>
#include "container.h"
#include "skipList.h"

class MemTable : public container {
private:
    SkipList table{};

public:
    MemTable() = default;
    ~MemTable() = default;

    void put(uint64_t key, const std::string& value, bool cover) override;
    std::string get(uint64_t key) override;
    void scan(uint64_t key1, uint64_t key2, SkipList *list) override;
    Buffer *Write(std::string &path, uint64_t stamp, uint64_t dup) const override;

    void reset();
};