#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "EventGenerator/KeyValueEventGenerator.h"

int main() {
    auto generator = new KeyValueEventGenerator(15);

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin", 15);
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvents(1000000, 10));
    return 0;
}
