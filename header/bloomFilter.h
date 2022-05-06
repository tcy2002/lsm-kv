#pragma once

#include <functional>
#include <cstring>
#include <cstdint>
#include <fstream>
#include "MurmurHash3.h"

typedef unsigned char byte;

class BFilter {
private:
    byte *table;
    int sizeOfFilter;
    int numOfHash;

public:
    BFilter(int s, int n, byte *t=nullptr): sizeOfFilter(s), numOfHash(n) {
        table = (t != nullptr ? t : new byte[s]{});
    }
    ~BFilter()=default;

    void insert(uint64_t key) {
        uint32_t hash[numOfHash];
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        byte mask;

        for (int i = 0; i < numOfHash; i++) {
            hash[i] %= (sizeOfFilter * 8);
            uint32_t remainder = hash[i] % 8, idx = hash[i] / 8;
            mask = 0x80 >> remainder;
            table[idx] |= mask;
        }
    }

    bool found(uint64_t key) {
        uint32_t hash[numOfHash];
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        byte mask;

        for (int i = 0; i < numOfHash; i++) {
            hash[i] %= (sizeOfFilter * 8);
            uint32_t remainder = hash[i] % 8, idx = hash[i] / 8;
            mask = 0x80 >> remainder;
            if (!(mask & table[idx])) return false;
        }

        return true;
    }

    byte *getTable() {return table;}
    void setTable(byte *t) {table = t;}
};