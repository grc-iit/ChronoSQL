#include <iostream>
#include "KeyValueEventWriterFactory.h"

int main() {
    auto *factory = new KeyValueEventWriterFactory();
    auto *writer = factory->getWriter();
    std::string args[1] = {"10000"};
    writer->writeToFile("test.bin", "payload", args);
    return 0;
}
