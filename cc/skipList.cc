#include <vector>
#include <fstream>
#include "../header/skipList.h"

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

/**
 * 在MemTable中插入一条数据
 * @param key 键
 * @param value 值
 * @param cover 是否需要覆盖已经存在的数据，默认为true
 */
uint64_t SkipList::Insert(uint64_t key, const std::string& value, bool cover)
{
    SKNode *x = head;
    SKNode *update[MAX_LEVEL]{};

    for (int i = MAX_LEVEL - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key)
            x = x->forwards[i];
        if (x->forwards[i]->key == key) {
            uint64_t size = x->forwards[i]->val.size();
            if (cover) {
                x->forwards[i]->val = value;
            }
            return size;
        }
        update[i] = x;
    }

    auto *newNode = new SKNode(key, value, NORMAL);
    int grow = randomLevel();
    for (int i = 0; i < grow; i++) {
        newNode->forwards[i] = update[i]->forwards[i];
        update[i]->forwards[i] = newNode;
    }

    return 0;
}

/**
 * 查询指定键对应的SKNode
 * @param key 键
 * @return SKNode指针
 */
SKNode *SkipList::Search(uint64_t key) const
{
    SKNode* x = head;
    for (int i = MAX_LEVEL - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
    }
    return x->forwards[0];
}

void SkipList::Reset() {
    SKNode *n1 = head->forwards[0];
    SKNode *n2;
    while (n1->key < NIL->key)
    {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = NIL;
    }
}
