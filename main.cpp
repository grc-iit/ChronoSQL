#include <iostream>
#include "Config/ConfigurationManager.h"
#include "EventGenerator/EventGeneratorFactory.h"
#include "EventWriter/EventWriterFactory.h"
#include "chronolog/ChronoLog.h"

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "Usage: poc.exe <path_to_config_file>" << std::endl;
        return -1;
    }

    auto *config = (new ConfigurationManager(argv[1]))->getConfiguration();

    auto *generator = new KeyValueEventGenerator(config->payloadSize, config->payloadVariation);

    auto *log = new ChronoLog(config);
    log->record(0, generator->generateRandomBytes(config->payloadSize));
    std::cout << log->playback() << std::endl;
    return 0;
}
