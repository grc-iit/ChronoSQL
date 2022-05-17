#include <iostream>
#include <random>
#include "EventWriter/KeyValueEventWriterFactory.h"

char *generateRandomChar(int size) {
    const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    char *result = new char[size];

    std::mt19937 rg{std::random_device{}()};
    std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(charset) - 2);

    for (int i = 0; i < size; i++) {
        result[i] = 'a' + charset[pick(rg)];
    }

    return result;
}

int main() {
    auto *factory = new KeyValueEventWriterFactory("test.bin");
    auto *writer = factory->getWriter();

    std::string args[1] = {"10000"};
    writer->writeToFile(generateRandomChar(10), args);
    return 0;
}
