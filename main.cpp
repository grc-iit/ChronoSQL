#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "EventGenerator/KeyValueEventGeneratorFactory.h"

int main() {
    auto *generatorFactory = new KeyValueEventGeneratorFactory(15, 10);
    auto *generator = generatorFactory->getGenerator();

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin", 15);
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvents(100));
    return 0;
}
