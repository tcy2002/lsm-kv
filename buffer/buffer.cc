#include "./buffer.h"

void BufferLevel::addToLinkByStamp(Buffer *p, BufferNode *root, int level) {
    while (root->next && root->next->buf->stamp > p->stamp) root = root->next;
    root->next = new BufferNode(p, root->next, level);
}

void BufferLevel::addToLinkByScope(Buffer *p, BufferNode *root, int level) {
    while (root->next && root->next->buf->max < p->min) root = root->next;
    root->next = new BufferNode(p, root->next, level);
}

/**
 * 将buffer插入到缓存列表中
 * @param level 需要插入的SSTable层级
 * @param p buffer指针
 */
void BufferLevel::addToBuffer(int level, Buffer *p) {
    while (level >= size) {
        buffer.push_back(new BufferNode);
        numOfLevel.push_back(0);
        size++;
    }

    addToLinkByStamp(p, buffer[level], level);
    numOfLevel[level]++;
}

/**
 * 从buffer列表中删除指定buffer
 * @param level 需要删除的SSTable所在层级
 * @param stamp 需要删除的SSTable时间戳
 * @param dup 需要删除的SSTable文件标签
 */
void BufferLevel::removeBuffer(int level, uint64_t stamp, uint64_t dup) {
    BufferNode *buf{buffer[level]}, *tmp;
    while (buf->next != nullptr) {
        if (buf->next->buf->stamp == stamp && buf->next->buf->dup == dup) {
            tmp = buf->next;
            buf->next = buf->next->next;
            delete tmp->buf;
            delete tmp;
            numOfLevel[level]--;
            break;
        }
        buf = buf->next;
    }
}

/**
 * 根据键值范围获取buffer，返回按键值升序排列的buffer列表
 * @param level 查询指定的SSTable层级
 * @param min 键值下界
 * @param max 键值上界
 * @return buffer列表头指针
 */
BufferNode *BufferLevel::findByScopeAndSortByScope(int level, uint64_t min, uint64_t max) {
    BufferNode *res{new BufferNode};
    if (level >= size) return res;

    BufferNode *buf{buffer[level]->next};
    while (true) {
        while (buf != nullptr && !(buf->buf->min <= max && buf->buf->max >= min)) buf = buf->next;
        if (buf == nullptr) break;
        addToLinkByScope(buf->buf, res, level);
        buf = buf->next;
    }

    return res;
}

/**
 * 根据键值范围获取buffer，返回按时间戳降序排列的buffer列表
 * @param key1 键值下界
 * @param key2 键值上界
 * @return buffer列表头指针
 */
BufferNode *BufferLevel::findByScopeAndSortByStamp(uint64_t key1, uint64_t key2) {
    BufferNode *buf, *res{new BufferNode}, *rear{res};

    for (auto it : buffer) {
        buf = it->next;
        while (true) {
            while (buf != nullptr && !(key2 >= buf->buf->min && key1 <= buf->buf->max))
                buf = buf->next;
            if (buf == nullptr) break;
            rear = rear->next = new BufferNode(buf->buf, nullptr, buf->level);
            buf = buf->next;
        }
    }

    return res;
}

/**
 * 获取k个时间戳最小的buffer，返回按时间戳降序排列的buffer列表
 * @param level 查询指定的SSTable层级
 * @param k 需要获取的buffer数目
 * @return buffer列表头指针
 */
BufferNode *BufferLevel::findByNumAndSortByStamp(int level, int k) {
    BufferNode *res{new BufferNode}, *buf{buffer[level]->next}, *rear{res};
    if (level >= size) return res;

    int pre{numOfLevel[level] - k};
    while (buf && pre-- > 0) buf = buf->next;
    while (buf != nullptr) {
        rear = rear->next = new BufferNode(buf->buf, nullptr, level);
        buf = buf->next;
    }

    return res;
}

/**
 * 获取k个时间戳最小的buffer，返回按键值升序排列的buffer列表
 * @param level 查询指定的SSTable层级
 * @param k 需要获取的buffer数目
 * @return buffer列表头指针
 */
BufferNode *BufferLevel::findByNumAndSortByScope(int level, int k) {
    BufferNode *res{new BufferNode}, *buf{buffer[level]->next};
    if (level >= size) return res;

    int pre{numOfLevel[level] - k};
    while (buf && pre-- > 0) buf = buf->next;
    while (buf != nullptr) {
        addToLinkByScope(buf->buf, res, level);
        buf = buf->next;
    }

    return res;
}

/**
 * 查询指定level已经包含的SSTable数目
 * @param level 需要查询的SSTable层级
 * @return SSTable数目
 */
int BufferLevel::getNumOfLevel(int level) {
    if (level >= size) return -1;
    return numOfLevel[level];
}

/**
 * 重置(清空)buffer列表
 */
void BufferLevel::Reset() {
    for (auto &it: buffer) delete it;
    buffer.clear();
    numOfLevel.clear();
    size = 0;
}
