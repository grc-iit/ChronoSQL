#include <iostream>
#include "Config/ConfigurationManager.h"
#include "EventGenerator/EventGeneratorFactory.h"
#include "EventWriter/EventWriterFactory.h"

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "Usage: poc.exe <path_to_config_file>" << std::endl;
        return -1;
    }

    auto *config = new ConfigurationManager(argv[1]);

    auto *generatorFactory = new EventGeneratorFactory();
    auto *generator = generatorFactory->getGenerator(config);

    auto *writerFactory = new EventWriterFactory();
    auto *writer = writerFactory->getWriter(config);

    writer->writeToFile(generator->generateEvents(config->nEvents));
    return 0;
}
