#include "../header/kvstore.h"

/**
 * 将MemTable中的内容写入SSTable
 * @param level 将要写入的SSTable层级
 * @param cStamp 将要写入的sst文件的时间戳
 */
void KVStore::writeToSSTable(int level, uint64_t cStamp) {
    std::string levelPath = LevelPath(level);
    if (!utils::dirExists(levelPath))
        utils::mkdir(levelPath.c_str());

    std::string path = FilePath(level, cStamp, currentDup);
    Buffer *bfn = MemTable.Write(path, cStamp, currentDup++);
    buffer.addToBuffer(level, bfn);
    MemTable.reset();
}

/**
 * 程序启动时从SSTable中读取除value之外的信息存入buffer
 */
void KVStore::readFromSSTable() {
    int currentLevel{};

    while (true) {
        std::string levelPath = LevelPath(currentLevel++);
        if (!utils::dirExists(levelPath)) break;
        std::vector<std::string> files;
        utils::scanDir(levelPath, files);

        for (auto &path : files) {
            std::ifstream ifs(path,std::ios::binary);

            //从文件名中解析时间戳与文件数
            char *p = strtok((char *)path.c_str(),"_");
            p = strtok(nullptr, "_");
            int cStamp = strtol(p, nullptr, 10);
            p = strtok(nullptr, "_");
            int cDup = strtol(p, nullptr, 10);
            if (cStamp > stamp) stamp = cStamp;
            if (cDup > currentDup) currentDup = cDup;

            uint64_t t, n, mi, ma;
            ifs.read((char *)&t, 8);
            ifs.read((char *)&n, 8);
            ifs.read((char *)&mi, 8);
            ifs.read((char *)&ma, 8);

            auto bfn = new Buffer(t, cDup, n, mi, ma);
            auto filter = new byte[BF_SIZE]{};
            ifs.read((char *)filter, BF_SIZE);
            bfn->filter->setTable(filter);

            uint64_t *&keys = bfn->keys;
            uint32_t *&offsets = bfn->offsets;
            for (int i = 0; i < bfn->num; i++) {
                ifs.read((char *)(keys + i), 8); //键值
                ifs.read((char *)(offsets + i), 4); //偏移量
            }
            //最后一个val结尾的offset
            ifs.read((char *)(offsets + bfn->num), 4);
            ifs.close();

            buffer.addToBuffer(currentLevel - 1, bfn);
        }
    }

    //设置当前的时间戳和文件数
    stamp++;
    currentDup++;
}

/**
 * 不使用buffer，直接在SSTable中查询键值。(用于性能测试)
 * @param key 键
 * @return 查询到的值
 */
std::string KVStore::searchSSTable(uint64_t key) {
    int currentLevel{};
    char *val;

    while (true) {
        std::string levelPath = LevelPath(currentLevel++);
        if (!utils::dirExists(levelPath)) break;
        std::vector<std::string> files;
        utils::scanDir(levelPath, files);

        if (currentLevel == 1) {
            std::map<uint64_t, std::string, std::greater<>> tmp;
            std::string str;

            while (!files.empty()) {
                str = files.back();
                char *p = strtok((char *)str.c_str(),"_");
                p = strtok(nullptr, "_");
                int cStamp = strtol(p, nullptr, 10);
                tmp.emplace(cStamp, files.back());
                files.pop_back();
            }

            for (auto &it : tmp) {
                files.emplace_back(it.second);
            }
        }

        for (auto &path : files) {
            std::ifstream ifs(path,std::ios::binary);

            uint64_t num, cKey;
            uint32_t offset1, offset2;
            long long pos{BF_SIZE + 20};
            uint32_t len;
            ifs.seekg(8);
            ifs.read((char *)&num, 8);

            for (int i = 0; i < num; i++) {
                pos += 12;
                ifs.seekg(pos);
                ifs.read((char *)&cKey, 8);
                if (cKey != key) continue;

                ifs.read((char *)&offset1, 4);
                ifs.seekg(pos + (i == num - 1 ? 12 : 20));
                ifs.read((char *)&offset2, 4);
                len = offset2 - offset1;
                val = new char[len + 1]{};

                ifs.seekg(offset1);
                ifs.read(val, len);

                ifs.close();
                return val;
            }

            ifs.close();
        }
    }

    return "";
}

/**
 * 根据文件流、位置、长度从一个SSTable中获取一条信息
 * @param ifs SSTable文件流
 * @param pos 需要读取的起始位置
 * @param len 需要读取的长度
 * @return 获得的信息
 */
std::string KVStore::getValueSSTable(std::ifstream &ifs, uint32_t pos, uint32_t len) {
    ifs.seekg(pos);
    char *value = new char[len + 1]{};
    ifs.read(value, len);
    std::string str(value);
    delete []value;

    return str;
}

/**
 * 根据文件流、位置、长度从一个SSTable中获取一系列信息
 * @param ifs SSTable文件流
 * @param pos 需要读取的起始位置列表
 * @param len 需要读取的长度列表
 * @param values 获得的信息列表
 */
void KVStore::getValuesSSTable(std::ifstream &ifs,
                               std::vector<uint32_t> &pos, std::vector<uint32_t> &len,
                               std::vector<std::string> &values) {
    char *value;
    int size = (int)pos.size();

    for (int i = 0; i < size; i++) {
        ifs.seekg(pos[i]);
        value = new char[len[i] + 1]{};
        ifs.read(value, len[i]);
        values.emplace_back(value);
        delete []value;
    }
}

/**
 * 删除指定的SSTable文件
 * @param level SSTable层级
 * @param t 时间戳
 * @param d 文件标签
 * @return
 */
int KVStore::removeSSTable(int level, uint64_t t, uint64_t d) {
    std::string path = FilePath(level, t, d);
    return utils::rmfile(path.c_str());
}