#include <iostream>
#include <unistd.h>
#include <filesystem>
#include "config/ConfigurationManager.h"
#include "event_generator/EventGeneratorFactory.h"
#include "chronolog/ChronoLog.h"
#include "parser/ChronoSQLParser.h"

void recordEvent(const CID &cid, const char *eid, const char *payload, EventWriter *eventWriter) {
    eventWriter->write(cid, new KeyValueEvent((time_t) strtol(eid, nullptr, 10),
                                              payload));
}

void recordEvent(const CID &cid, const char *payload, EventWriter *eventWriter) {
    eventWriter->write(cid, new KeyValueEvent(std::time(nullptr), payload));
}

void generateEvents(ConfigurationValues *config, char **argv) {
    // Event generation
    auto *generator = new KeyValueEventGenerator(config->payloadSize, config->payloadVariation,
                                                 config->lowerTimestamp, config->higherTimestamp);
    auto *writerFactory = new EventWriterFactory();
    EventWriter *eventWriter = writerFactory->getWriter(config);

    std::list<Event *> events = generator->generateEvents(strtol(argv[4], nullptr, 10));

    events.sort([](const Event *event1, const Event *event2) {
        return event1->getTimestamp() < event2->getTimestamp();
    });

    eventWriter->write(argv[3], events);
}

int mainLoop(ConfigurationValues *config) {
    auto *parser = new ChronoSQLParser(config);
    std::string command;

    std::cout << "ChronoSQL version 1.0.0" << std::endl << "Type \"help\" for help." << std::endl;

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

    if (argc > 2) {
        if (!strcmp(argv[2], "-g")) {
            // Generate events
            generateEvents(config, argv);
        } else if (!strcmp(argv[2], "-i")) {
            // Bring indexes to memory
            for (int i = 3; i < argc; i++) {
                std::string buf(argv[i]);
                MemoryIndex::generate(buf);
            }
            return mainLoop(config);
        } else {
            std::cout << "Invalid arguments" << std::endl;
            return -1;
        }

        return 0;
    }

    return mainLoop(config);
}
