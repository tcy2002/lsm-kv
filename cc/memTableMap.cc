/*
 * 用于跳表和STL map的对比测试
 */
#include "../header/memTableMap.h"

void MemTableMap::put(uint64_t key, const std::string &value, bool cover) {
    if (table.emplace(key, value).second) {
        num++;
        mem += (12 + value.size());
        if (key > max) max = key;
        if (key < min) min = key;
    } else if (cover) {
        auto it = table.find(key);
        uint64_t size = it->second.size();
        table[key] = value;
        mem += (value.size() - size);
    }
}

std::string MemTableMap::get(uint64_t key) {
    auto it = table.find(key);
    return it == table.end() ? "" : it->second;
}

void MemTableMap::scan(uint64_t key1, uint64_t key2, SkipList *list) {
    for (auto &it : table) {
        if (it.first >= key1) {
            list->Insert(it.first, it.second);
        } else if (it.first > key2) return;
    }
}

Buffer *MemTableMap::Write(std::string &path, uint64_t stamp, uint64_t dup) const {
    std::ofstream ofs(path, std::ios::binary);

    //head
    ofs.write((char *)&stamp, 8);
    ofs.write((char *)&num, 8);
    ofs.write((char *)&min, 8);
    ofs.write((char *)&max, 8);

    //bloomFilter
    BFilter bf(BF_SIZE, 4);
    for (auto &it : table) {
        bf.insert(it.first);
    }
    ofs.write((char *)bf.getTable(), BF_SIZE);
    auto *bfn = new Buffer(stamp, dup, num, min, max, bf.getTable());

    //键与偏移量
    uint64_t *&keys = bfn->keys;
    uint32_t *&offsets = bfn->offsets;
    uint32_t offset = BF_SIZE + 36 + num * 12, idx{};
    for (auto &it : table) {
        keys[idx] = it.first;
        offsets[idx++] = offset;
        ofs.write((char *)&it.first, 8);
        ofs.write((char *)&offset, 4);
        offset += (it.second).size();
    }
    //便于计算最后一条数据的长度
    offsets[idx] = offset;
    ofs.write((char *)&offset, 4);

    for (auto &it : table) {
        ofs.write(it.second.c_str(), (int64_t)it.second.size());
    }

    ofs.close();
    return bfn;
}

void MemTableMap::reset() {
    resetData();
    table.clear();
}
