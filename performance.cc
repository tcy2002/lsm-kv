#include <iostream>
#include <windows.h>
#include <random>
#include <ctime>
#include "kvstore.h"

void performanceTest(int size, int dup) {
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<int> u(100, 1000);

    LARGE_INTEGER freq_;
    QueryPerformanceFrequency(&freq_);
    LARGE_INTEGER begin_time;
    LARGE_INTEGER end_time;
    KVStore store("../data");
    double latency1{}, latency2{}, latency3{};
    double throughput1{}, throughput2{}, throughput3{};

    for (int i = 0; i < dup; i++) {
        store.reset();
        QueryPerformanceCounter(&begin_time);
        for (int j = 0; j < size; j++) store.put(j, std::string(u(e), 'a'));
        QueryPerformanceCounter(&end_time);
        latency1 += (double)(end_time.QuadPart - begin_time.QuadPart) * 1000000.0 / (double)freq_.QuadPart;
        throughput1 += size / ((double)(end_time.QuadPart - begin_time.QuadPart) / (double)freq_.QuadPart);

        QueryPerformanceCounter(&begin_time);
        for (int j = 0; j < size; j++) store.del(j);
        QueryPerformanceCounter(&end_time);
        latency2 += (double)(end_time.QuadPart - begin_time.QuadPart) * 1000000.0 / (double)freq_.QuadPart;
        throughput2 += size / ((double)(end_time.QuadPart - begin_time.QuadPart) / (double)freq_.QuadPart);
    }

    store.reset();
    for (int j = 0; j < size; j++) store.put(j, std::string(u(e), 'a'));
    for (int i = 0; i < dup; i++) {
        QueryPerformanceCounter(&begin_time);
        for (int j = 0; j < size; j++) store.get(j);
        QueryPerformanceCounter(&end_time);
        latency3 += (double)(end_time.QuadPart - begin_time.QuadPart) * 1000000.0 / (double)freq_.QuadPart;
        throughput3 += size / ((double)(end_time.QuadPart - begin_time.QuadPart) / (double)freq_.QuadPart);
    }

    std::cout << "Average latency for scope " << size << ":" << std::endl;
    std::cout << "put: " << latency1 / dup / size << "us" << std::endl;
    std::cout << "del: " << latency2 / dup / size << "us" << std::endl;
    std::cout << "get: " << latency3 / dup / size << "us" << std::endl;
    std::cout << "Average throughput for scope " << size << ":" << std::endl;
    std::cout << "put: " << (int)(throughput1 / dup) << "/s" << std::endl;
    std::cout << "del: " << (int)(throughput2 / dup) << "/s" << std::endl;
    std::cout << "get: " << (int)(throughput3 / dup) << "/s" << std::endl;
    std::cout << std::endl;
}

int main() {
    int dup{10};
    performanceTest(100, dup);
    performanceTest(1000, dup);
    performanceTest(5000, dup);
    performanceTest(10000, dup);
    performanceTest(20000, dup);
    return 0;
}