#pragma once

#include <iostream>
#include <utility>
#include <vector>
#include <climits>
#include <ctime>
#include "../bloomFilter/bloomFilter.h"
#include "../buffer/buffer.h"

#define MAX_LEVEL 15
#define MAX_NIL INT64_MAX
typedef unsigned char byte;

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    SKNode **forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
            : key(_key), val(std::move(_val)), type(_type)
    {
        forwards = new SKNode*[MAX_LEVEL];
        for (int i = 0; i < MAX_LEVEL; ++i)
            forwards[i] = nullptr;
    }
};

class SkipList
{
private:
    SKNode *head;
    SKNode *NIL;
    uint64_t s = 1;
    double my_rand();
    int randomLevel();

public:
    SkipList() {
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(MAX_NIL, "", SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i) head->forwards[i] = NIL;
    }
    ~SkipList()
    {
        Reset();
        delete head;
        delete NIL;
    }

    uint64_t Insert(uint64_t key, const std::string& value, bool cover=true);
    SKNode *Search(uint64_t key) const;
    void Reset();
};
