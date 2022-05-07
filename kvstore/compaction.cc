#include "../kvstore.h"

//删除SSTable与buffer的宏定义
#define Remove(p, l) \
while ((p) != nullptr) { \
removeSSTable((l), (p)->buf->stamp, (p)->buf->dup); \
buffer.removeBuffer((l), (p)->buf->stamp, (p)->buf->dup); \
BufferNode *t = p;   \
(p) = (p)->next;     \
delete t;\
}

//归并时将数据写入新SSTable的宏定义
#define Rewrite(l, buf, ptr, file) \
uint32_t *&offsets{(buf)->offsets}; \
int &id{(ptr)};        \
uint64_t key = (buf)->keys[id];              \
uint32_t pos = offsets[id];                 \
uint32_t len = offsets[id + 1] - offsets[id];        \
std::string val = getValueSSTable(file, pos, len);   \
put(key, val, (l), maxStamp);

/**
 * 第0层compaction
 */
void KVStore::compaction() {
    std::vector<std::ifstream> files;
    std::vector<BufferNode *> buf;
    std::vector<int> ptr{0, 0, 0};
    int size{3};
    uint64_t min{INT64_MAX}, max{0}, maxStamp;

    //一路归并完成时更新数据状态的宏定义
#define UpdateMem(i)  \
files[(i)].close();  \
files.erase(files.begin() + (i)); \
if (buf[(i)]->level == 0) {      \
    buf.erase(buf.begin() + (i)); \
    ptr.erase(ptr.begin() + (i)); \
    size--;\
} else {               \
    buf[(i)] = buf[(i)]->next;    \
    ptr[(i)] = 0;      \
    if (buf[(i)] == nullptr) {    \
        buf.pop_back();\
        ptr.pop_back();\
        size--;\
    }\
}

    //获取level0中需要归并的数据
    BufferNode *level0{buffer.findByNumAndSortByStamp(0, 3)}, *p{level0->next};
    maxStamp = p->buf->stamp;
    while (p != nullptr) {
        buf.push_back(p);
        std::string path = FilePath(p->level, p->buf->stamp, p->buf->dup);
        files.emplace_back(path, std::ios::binary);
        if (p->buf->min < min) min = p->buf->min;
        if (p->buf->max > max) max = p->buf->max;
        p = p->next;
    }

    //获取level1中需要归并的数据（可能不存在）
    BufferNode *level1{buffer.findByScopeAndSortByScope(1, min, max)};
    p = level1->next;
    if (p != nullptr) {
        size++;
        buf.push_back(p);
        ptr.push_back(0);
        while (p != nullptr) {
            std::string path = FilePath(p->level, p->buf->stamp, p->buf->dup);
            files.emplace_back(path, std::ios::binary);
            p = p->next;
        }
    }

    uint32_t idx;
    uint64_t tmp;

    //四路（三路）归并
    while (true) {
        min = INT64_MAX;
        for (int i = 0; i < size; i++) {
            tmp = buf[i]->buf->keys[ptr[i]];
            if (tmp < min) { min = tmp; idx = i; }
            else if (tmp == min && ++ptr[i] >= buf[i]->buf->num)
            { UpdateMem(i) }
        }

        Rewrite(1, buf[idx]->buf, ptr[idx], files[idx])
        if (++ptr[idx] >= buf[idx]->buf->num) { UpdateMem(idx) }
        if (size == 0) break;
    }
    if (!MemTable.isEmpty()) writeToSSTable(1, maxStamp);

    //删除SSTable和buffer
    p = level0->next;
    Remove(p, 0)
    p = level1->next;
    Remove(p, 1)

    size = buffer.getNumOfLevel(1);
    if (size > 4) compaction(1, size - 4);
}

/**
 * 第k层compaction(k!=0)
 * @param level 需要归并的层级
 * @param k 需要归并的SSTable数目
 */
void KVStore::compaction(int level, int k) {
    std::vector<std::ifstream> files[2]{};
    BufferNode *buf[2];
    int ptr[2]{0, 0}, size[2]{k, 0};
    uint64_t min, max, maxStamp{};

    //一路归并完成时更新数据状态的宏定义
#define UpdateMemN(i) \
files[(i)][0].close(); \
files[(i)].erase(files[(i)].begin()); \
buf[(i)] = buf[(i)]->next; \
ptr[(i)] = 0; \
size[(i)]--;

    //获取levelN中需要归并的数据
    BufferNode *levelN{buffer.findByNumAndSortByScope(level, k)}, *p{levelN->next};
    min = p->buf->min;
    while (p != nullptr) {
        std::string path = FilePath(p->level, p->buf->stamp, p->buf->dup);
        files[0].emplace_back(path, std::ios::binary);
        max = p->buf->max;
        if (p->buf->stamp > maxStamp) maxStamp = p->buf->stamp;
        p = p->next;
    }

    //获取levelN+1中需要归并的数据
    BufferNode *levelN1{buffer.findByScopeAndSortByScope(level + 1, min, max)};
    p = levelN1->next;
    while (p != nullptr) {
        std::string path = FilePath(p->level, p->buf->stamp, p->buf->dup);
        files[1].emplace_back(path, std::ios::binary);
        size[1]++;
        p = p->next;
    }

    buf[0] = levelN->next;
    buf[1] = levelN1->next;
    uint64_t key1, key2;
    int idx;

    //二路归并
    while (true) {
        if (size[0] > 0 && size[1] > 0) {
            key1 = buf[0]->buf->keys[ptr[0]];
            key2 = buf[1]->buf->keys[ptr[1]];
            if (key1 <= key2) {
                idx = 0;
                if (key1 == key2 && ++ptr[1] >= buf[1]->buf->num)
                { UpdateMemN(1) }
            }
            else idx = 1;
        } else if (size[0] > 0) idx = 0;
        else if (size[1] > 0) idx = 1;
        else break;

        Rewrite(level + 1, buf[idx]->buf, ptr[idx], files[idx][0])
        if (++ptr[idx] >= buf[idx]->buf->num) { UpdateMemN(idx) }
    }
    if (!MemTable.isEmpty()) writeToSSTable(level + 1, maxStamp);

    //删除SSTable和buffer
    p = levelN->next;
    Remove(p, level)
    p = levelN1->next;
    Remove(p, level + 1)

    int current = buffer.getNumOfLevel(level + 1);
    int limit = (int)pow(2, level + 2);
    if (current > limit) compaction(level + 1, current - limit);
}