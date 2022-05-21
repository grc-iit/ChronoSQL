#include <iostream>
#include "Config/ConfigurationManager.h"
#include "EventGenerator/EventGeneratorFactory.h"
#include "EventWriter/EventWriterFactory.h"

int main() {
    auto *config = new ConfigurationManager("../config.json");

    auto *generatorFactory = new EventGeneratorFactory();
    auto *generator = generatorFactory->getGenerator(config);

    auto *writerFactory = new EventWriterFactory();
    auto *writer = writerFactory->getWriter(config);

    writer->writeToFile(generator->generateEvents(100));
    return 0;
}
