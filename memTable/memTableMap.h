#pragma once

#include "container.h"
#include <map>

class MemTableMap : public container {
private:
    std::map<uint64_t, std::string> table{};

public:
    MemTableMap() = default;
    ~MemTableMap() = default;

    void put(uint64_t key, const std::string& value, bool cover) override;
    std::string get(uint64_t key) override;
    void scan(uint64_t key1, uint64_t key2, SkipList *list) override;
    Buffer *Write(std::string &path, uint64_t stamp, uint64_t dup) const override;

    void reset();
};