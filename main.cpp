#include <iostream>
#include <unistd.h>
#include "config/ConfigurationManager.h"
#include "event_generator/EventGeneratorFactory.h"
#include "chronolog/ChronoLog.h"
#include "parser/ChronoSQLParser.h"

int mainLoop(EventReader *eventReader) {
    auto *parser = new ChronoSQLParser(eventReader);
    std::string command;

    std::cout << "ChronoSQL version 0.0.1" << std::endl << "Type \"help\" for help." << std::endl;

    while (true) {
        if (isatty(STDIN_FILENO)) {
            std::cout << "csql>";
        }   // else: no prompt for non-interactive sessions

        std::getline(std::cin, command);

        if (command == "help") {
            std::cout << "Available commands:" << std::endl << std::endl
                      << "exit: close the ChronoSQL interactive shell"
                      << std::endl << "help: print help" << std::endl << "q: close the ChronoSQL interactive shell"
                      << std::endl << std::endl << "More coming soon!" << std::endl;
        } else if (command == "exit" || command == "q") {
            break;
        } else {
            parser->parse(command);
        }
    }

    std::cout << std::endl << "Exiting...\n" << std::endl;
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "Usage: csql.exe <path_to_config_file>" << std::endl;
        return -1;
    }

    auto *config = (new ConfigurationManager(argv[1]))->getConfiguration();

    auto *generator = new KeyValueEventGenerator(config->payloadSize, config->payloadVariation);

    auto *log = new ChronoLog(config);
    std::string cid = "test";
    log->record(cid, generator->generateRandomBytes(config->payloadSize));
    log->record(cid, generator->generateRandomBytes(config->payloadSize));
    log->record(cid, generator->generateRandomBytes(config->payloadSize));
    log->record(cid, generator->generateRandomBytes(config->payloadSize));
    log->record(cid, generator->generateRandomBytes(config->payloadSize));
    std::cout << log->playback(cid) << std::endl;

//    time_t timenum = (time_t) strtol(timestr, NULL, 10);

    std::list<char *> *events = log->replay(cid, VOID_TIMESTAMP, VOID_TIMESTAMP);

    int i = 0;
    for (auto &event: *events) {
        std::cout << i++ << ": " << event << std::endl;
    }

    // Debug dump
    MemoryEventStorage::dumpContents();

    auto *reader = (new EventReaderFactory())->getReader(config);

    return mainLoop(reader);
}
