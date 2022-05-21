#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "EventGenerator/KeyValueEventGeneratorFactory.h"
#include "Config/ConfigurationManager.h"

int main() {
    auto config = new ConfigurationManager("../config.json");

    auto *generatorFactory = new KeyValueEventGeneratorFactory(15, 10);
    auto *generator = generatorFactory->getGenerator();

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin", 15);
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvents(100));
    return 0;
}
