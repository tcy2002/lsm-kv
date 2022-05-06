#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <io.h>
#include <cmath>
#include <map>
#include "utils.h"
#include "kvstore_api.h"
#include "memTable.h"
#include "memTableMap.h"
#include "buffer.h"

#define EMPTY ""
#define DELETED "~DELETED~"
#define MAX_MEM 2 * 1024 * 1024
#define LevelPath(l) (dir + "/level-" + std::to_string((l)))
#define FilePath(l, t, d) (LevelPath((l)) \
+ "/_" + std::to_string((t)) \
+ "_" + std::to_string((d)) \
+ "_.sst")

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    std::string dir;
    uint64_t stamp;
    MemTableMap MemTable{};
    BufferLevel buffer{};
    uint64_t currentDup;

    void writeToSSTable(int level, uint64_t stamp);
    void readFromSSTable();
    std::string searchBuffer(uint64_t key);

    std::string searchSSTable(uint64_t key);
    static std::string getValueSSTable(std::ifstream &ifs, uint32_t pos, uint32_t len);
    static void getValuesSSTable(std::ifstream &ifs, std::vector<uint32_t> &pos, std::vector<uint32_t> &len, std::vector<std::string> &values);
    int removeSSTable(int level, uint64_t t, uint64_t d);
    void compaction();
    void compaction(int level, int k);
    void put(uint64_t key, const std::string &s, int level, uint64_t sStamp);

public:
	explicit KVStore(const std::string &dir);
	~KVStore();

	void put(uint64_t key, const std::string &s) override;
	std::string get(uint64_t key) override;
	bool del(uint64_t key) override;
	void reset() override;
	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
