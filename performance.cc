#include <iostream>
#include <windows.h>
#include <random>
#include <ctime>
#include "header/kvstore.h"

int main() {
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<int> u(100, 1000);

    LARGE_INTEGER freq_;
    QueryPerformanceFrequency(&freq_);
    LARGE_INTEGER begin_time;
    LARGE_INTEGER end_time;
    KVStore store("../data");
    int size{20000};
    store.reset();
    std::list<std::pair<uint64_t, std::string> > li;

    QueryPerformanceCounter(&begin_time);
    for (int i = 0; i < size; i++) store.put(i, std::string(u(e), 'a'));
    int start = 9000;
    for (int i = start; i < start + 700; i++) {
        if (store.get(i).empty()) return i;
    }
    QueryPerformanceCounter(&end_time);

    std::cout << (double)(end_time.QuadPart - begin_time.QuadPart) * 1000.0 / (double)freq_.QuadPart << "ms" << std::endl;

    return 0;
}