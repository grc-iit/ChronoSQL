#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "EventGenerator/KeyValueEventGenerator.h"

int main() {
    auto generator = new KeyValueEventGenerator(10);

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin", 10);
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvent());
    return 0;
}
