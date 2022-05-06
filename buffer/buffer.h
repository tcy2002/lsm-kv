#pragma once

#include <vector>
#include "../bloomFilter/bloomFilter.h"

#define BF_SIZE 10 * 1024
typedef unsigned char byte;

struct Buffer {
    uint64_t stamp;
    uint64_t dup;
    uint64_t num;
    uint64_t min;
    uint64_t max;
    BFilter *filter;
    uint64_t *keys;
    uint32_t *offsets;

    Buffer(uint64_t t, uint64_t d, uint64_t n=1, uint64_t mi=INT64_MAX, uint64_t ma=0, byte *f=nullptr):
    stamp(t), dup(d), num(n), min(mi), max(ma)
    {
        filter = new BFilter(BF_SIZE, 4, f);
        keys = new uint64_t[n]{};
        offsets = new uint32_t[n + 1]{};
    }
    ~Buffer() {
        delete filter;
        delete []keys;
        delete []offsets;
    }
};

struct BufferNode {
    Buffer *buf;
    BufferNode *next;
    int level;

    explicit BufferNode(Buffer *b=nullptr, BufferNode *bn=nullptr, int l=-1):
    buf(b), next(bn), level(l) {}
    ~BufferNode() = default;
};

class BufferLevel {
private:
    std::vector<BufferNode *> buffer;
    std::vector<int> numOfLevel;
    int size{};

    static void addToLinkByStamp(Buffer *p, BufferNode *root, int level);
    static void addToLinkByScope(Buffer *p, BufferNode *root, int level);

public:
    BufferLevel() = default;
    ~BufferLevel() = default;

    void addToBuffer(int level, Buffer *p);
    void removeBuffer(int level, uint64_t stamp, uint64_t dup);
    BufferNode *findByScopeAndSortByScope(int level, uint64_t min, uint64_t max);
    BufferNode *findByScopeAndSortByStamp(uint64_t key1, uint64_t key2);
    BufferNode *findByNumAndSortByStamp(int level, int k);
    BufferNode *findByNumAndSortByScope(int level, int k);
    int getNumOfLevel(int level);
    int getNumOfLevels() const { return size; }
    void Reset();
};