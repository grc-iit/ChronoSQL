#include <iostream>
#include "config/ConfigurationManager.h"
#include "eventGenerator/EventGeneratorFactory.h"
#include "chronolog/ChronoLog.h"

int mainLoop() {
    std::string command;

    std::cout << "ChronoSQL version 0.0.1" << std::endl << "Type \"help\" for help." << std::endl;

    while (true) {
        std::cout << "csql>";
        std::getline(std::cin, command);

        if (command == "help") {
            std::cout << "Available commands:" << std::endl << std::endl
                      << "exit: close the ChronoSQL interactive shell"
                      << std::endl << "help: print help" << std::endl << "q: close the ChronoSQL interactive shell"
                      << std::endl << std::endl << "More coming soon!" << std::endl;
        } else if (command == "exit" || command == "q") {
            break;
        }
    }

    std::cout << std::endl << "Exiting...\n" << std::endl;
    return 0;
}

int main(int argc, char **argv) {
//    if (argc < 2) {
//        std::cout << "Usage: poc.exe <path_to_config_file>" << std::endl;
//        return -1;
//    }
//
//    auto *config = (new ConfigurationManager(argv[1]))->getConfiguration();
//
//    auto *generator = new KeyValueEventGenerator(config->payloadSize, config->payloadVariation);
//
//    auto *log = new ChronoLog(config);
//    log->record(0, generator->generateRandomBytes(config->payloadSize));
//    log->record(0, generator->generateRandomBytes(config->payloadSize));
//    log->record(0, generator->generateRandomBytes(config->payloadSize));
//    log->record(0, generator->generateRandomBytes(config->payloadSize));
//    log->record(0, generator->generateRandomBytes(config->payloadSize));
//    std::cout << log->playback() << std::endl;
//
//    // time_t timenum = (time_t) strtol(timestr, NULL, 10);
//
//    std::list<char *> events = log->replay(-1, -1);
//
//    int i = 0;
//    for (auto &event: events) {
//        std::cout << i++ << ": " << event << std::endl;
//    }
//
//    return 0;
    return mainLoop();
}
