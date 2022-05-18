#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "EventGenerator/KeyValueEventGenerator.h"

int main() {
    auto generator = new KeyValueEventGenerator();

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin");
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvent());
    return 0;
}
