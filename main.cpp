#include <iostream>
#include "EventWriter/KeyValueEventWriterFactory.h"
#include "Config/ConfigurationManager.h"
#include "EventGenerator/EventGeneratorFactory.h"

int main() {
    auto *config = new ConfigurationManager("../config.json");

    auto *generatorFactory = new EventGeneratorFactory();
    auto *generator = generatorFactory->getGenerator(config);

    auto *writerFactory = new KeyValueEventWriterFactory("test.bin", 15);
    auto *writer = writerFactory->getWriter();

    writer->writeToFile(generator->generateEvents(100));
    return 0;
}
