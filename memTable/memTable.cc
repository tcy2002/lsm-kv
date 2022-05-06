#include "./memTable.h"

void MemTable::put(uint64_t key, const std::string &value, bool cover) {
    uint64_t size = table.Insert(key, value, cover);
    if (size == 0) {
        num++;
        mem += (12 + value.size());
        if (key > max) max = key;
        if (key < min) min = key;
    } else if (cover) {
        mem += (value.size() - size);
    }
}

std::string MemTable::get(uint64_t key) {
    SKNode *res = table.Search(key);
    return (res->key == key ? res->val : "");
}

void MemTable::scan(uint64_t key1, uint64_t key2, SkipList *list) {
    SKNode *start = table.Search(key1);
    while (start->key <= key2) {
        list->Insert(start->key, start->val); //不覆盖
        start = start->forwards[0];
    }
}

Buffer *MemTable::Write(std::string &path, uint64_t stamp, uint64_t dup) const {
    std::ofstream ofs(path, std::ios::binary);

    //head
    ofs.write((char *)&stamp, 8);
    ofs.write((char *)&num, 8);
    ofs.write((char *)&min, 8);
    ofs.write((char *)&max, 8);

    SKNode *head = table.Search(0);

    //bloomFilter
    SKNode *p = head;
    BFilter bf(BF_SIZE, 4);
    while (p->key < MAX_NIL) {
        bf.insert(p->key);
        p = p->forwards[0];
    }
    ofs.write((char *)bf.getTable(), BF_SIZE);
    auto *bfn = new Buffer(stamp, dup, num, min, max, bf.getTable());

    //键与偏移量
    uint64_t *&keys = bfn->keys;
    uint32_t *&offsets = bfn->offsets;
    uint32_t offset = BF_SIZE + 36 + num * 12, idx{};
    p = head;
    while (p->key < MAX_NIL) {
        keys[idx] = p->key;
        offsets[idx++] = offset;
        ofs.write((char *)&p->key, 8);
        ofs.write((char *)&offset, 4);
        offset += (p->val).size();
        p = p->forwards[0];
    }
    //便于计算最后一条数据的长度
    offsets[idx] = offset;
    ofs.write((char *)&offset, 4);

    p = head;
    while (p->key < MAX_NIL) {
        ofs.write(p->val.c_str(), (int64_t)p->val.size());
        p = p->forwards[0];
    }

    ofs.close();
    return bfn;
}

void MemTable::reset() {
    resetData();
    table.Reset();
}

