#include "./kvstore.h"

KVStore::KVStore(const std::string &dir):
KVStoreAPI(dir), stamp(0), currentDup(0), dir(dir)
{
    readFromSSTable();
}

KVStore::~KVStore() {
    if (!MemTable.isEmpty())
        writeToSSTable(0, stamp);
    if (buffer.getNumOfLevel(0) > 2) compaction();
    buffer.Reset();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (MemTable.getMem() + 12 + s.size() > MAX_MEM) {
        writeToSSTable(0, stamp++);
        if (buffer.getNumOfLevel(0) > 2) compaction();
    }

    MemTable.put(key, s, true);
}

/**
 * 用于compaction中的数据插入函数
 * @param key 键
 * @param s 值
 * @param level 将要写入的SSTable层级
 * @param cStamp 将要写入的sst文件的时间戳
 */
void KVStore::put(uint64_t key, const std::string &s, int level, uint64_t cStamp)
{
    //最后一层compaction时删除DELETED标签
    if (s == DELETED && level == buffer.getNumOfLevels() - 1) return;

    if (MemTable.getMem() + 12 + s.size() > MAX_MEM)
        writeToSSTable(level, cStamp);

    MemTable.put(key, s, false);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string str = MemTable.get(key);
    if (str.empty()) str = searchBuffer(key);
    return str != DELETED ? str : EMPTY;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if (!get(key).empty()) {
        put(key, DELETED);
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    MemTable.reset();
    buffer.Reset();
    stamp = 1;
    currentDup = 1;

    //删除所有SSTable
    int currentLevel = 0;
    std::vector<std::string> files;
    while (true) {
        std::string levelPath = LevelPath(currentLevel++);
        if (!utils::dirExists(levelPath)) break;
        utils::scanDir(levelPath, files);
        for (std::string& file : files) utils::rmfile(file.c_str()); //删除文件
        utils::rmdir(levelPath.c_str()); //删除文件夹
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    SkipList tmpList{}; //利用跳表暂存数据

    //从跳表中读取数据
    MemTable.scan(key1, key2, &tmpList);

    //从SSTable中读取数据，found为包含该键值范围的所有buffer的链表
    BufferNode *found = buffer.findByScopeAndSortByStamp(key1, key2), *buf{found->next};
    int cPos, cSize;

    while (buf != nullptr) {
        cPos = cSize = 0;
        std::vector<uint64_t> key;
        std::vector<uint32_t> pos, len;
        std::vector<std::string> values;

        uint64_t *&keys = buf->buf->keys;
        uint32_t *&offsets = buf->buf->offsets;
        while (keys[cPos] < key1) cPos++;
        while (cPos < buf->buf->num && keys[cPos] <= key2) {
            key.push_back(keys[cPos]);
            pos.push_back(offsets[cPos]);
            len.push_back(offsets[cPos + 1] - offsets[cPos]);
            cPos++;
            cSize++;
        }

        std::string path = FilePath(buf->level, buf->buf->stamp, buf->buf->dup);
        std::ifstream ifs(path, std::ios::binary);
        getValuesSSTable(ifs, pos, len, values);
        ifs.close();

        for (int i = 0; i < cSize; i++)
            tmpList.Insert(key[i], values[i], false);
        buf = buf->next;
    }

    delete found;

    //将跳表写入list
    SKNode *skn = tmpList.Search(0);
    while (skn->key <= key2) {
        if (skn->val != DELETED) list.emplace_back(skn->key, skn->val);
        skn = skn->forwards[0];
    }
}

/**
 * 在buffer中查询键值，并在对应的SSTable中获取value
 * @param key 键
 * @return 查询到的值
 */
std::string KVStore::searchBuffer(uint64_t key) {
    BufferNode *found{buffer.findByScopeAndSortByStamp(key, key)}, *buf{found->next};
    int res{-1};

    while (buf != nullptr) {
        //首先在bloomFilter中查找，如果没有直接跳过
        if (!buf->buf->filter->found(key)) {
            buf = buf->next;
            continue;
        }

        //二分法搜索键值
        uint64_t *&keys = buf->buf->keys;
        uint64_t low = 0, high = buf->buf->num - 1, mid = (low + high) / 2;
        uint64_t sKey;
        while (high - low > 1) {
            sKey = keys[mid];
            if (key > sKey) { low = mid; mid = (mid + high) / 2; }
            else if (key < sKey) { high = mid; mid = (mid + low) / 2; }
            else break;
        }

        if (key == keys[mid]) res = (int)mid;
        else if (key == keys[low]) res = (int)low;
        else if (key == keys[high]) res = (int)high;
        //由于bloomFilter存在一定的误报率，二分查找未找到并不意味着不存在，继续在下一个buffer中寻找
        else { buf = buf->next; continue; }
        break;
    }

    //如果最终仍然没有，则返回空
    if (res == -1) { delete found; return EMPTY; }

    uint32_t pos = buf->buf->offsets[res];
    uint32_t len = buf->buf->offsets[res + 1] - pos;
    std::string path = FilePath(buf->level, buf->buf->stamp, buf->buf->dup);
    std::ifstream ifs(path, std::ios::binary);
    std::string val = getValueSSTable(ifs, pos, len);
    ifs.close();

    delete found; return val;
}
