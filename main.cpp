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

std::string replaceAll(std::string str, const std::string &from, const std::string &to) {
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}

int generateEvents(ConfigurationValues *config, int argc, char **argv) {
    // Event generation
    auto *generator = new KeyValueEventGenerator(config->payloadSize, config->payloadVariation,
                                                 config->lowerTimestamp, config->higherTimestamp);
    auto *writerFactory = new EventWriterFactory();
    EventWriter *eventWriter = writerFactory->getWriter(config);

    for (int i = 3; i < (argc - 1); i += 2) {
        std::list<Event *> events = generator->generateEvents(strtol(argv[i + 1], nullptr, 10));

        events.sort([](const Event *event1, const Event *event2) {
            return event1->getTimestamp() < event2->getTimestamp();
        });

        eventWriter->write(argv[i], events);
    }

    std::cout << "Generation of events finished" << std::endl;

    if (!strcmp(argv[argc - 1], "-t")) {
        return 1;
    }

    return 0;
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

void
writeResults(std::unordered_map<int, std::list<double>> results, const std::list<std::string> &statements,
             const std::string &cidPrefix, std::unordered_map<int, std::string> suffixes) {
    std::ofstream file("results_" + cidPrefix + ".csv");
    file << " ,";

    int i = 0;
    for (auto st: statements) {
        file << "q" << i << ",";
        i++;
    }
    file << "\n";

    for (i = 10000; i <= 10000000; i *= 10) {
        file << suffixes[i] << ",";
        for (double value: results[i]) {
            file << value << ",";
            std::cout << value << std::endl;
        }
        file << "\n";
    }
    file.close();
}

int executeTests(ConfigurationValues *config) {
    std::string cidPrefix;
    if (config->eventType == EventType::MEMORY_KEY_VALUE) {
        cidPrefix = "mem";
    } else if (config->eventType == EventType::FIXED_KEY_VALUE) {
        cidPrefix = "naive";
    } else if (config->eventType == EventType::INDEXED_KEY_VALUE) {
        cidPrefix = "indexed";
    }

    auto *parser = new ChronoSQLParser(config);
    std::ifstream sqlFile(config->sqlFilePath);
    std::string statement;
    std::unordered_map<int, std::string> suffixes = {{10000,    "_10k"},
                                                     {100000,   "_100k"},
                                                     {1000000,  "_1m"},
                                                     {10000000, "_10m"}};
    std::unordered_map<int, std::list<double>> results;

    auto statements = std::list<std::string>();

    while (std::getline(sqlFile, statement)) {
        statements.push_back(statement);
    }

    for (int i = 10000; i <= 10000000; i *= 10) {
        results[i] = std::list<double>();
        for (std::string st: statements) {
            double diff = 0;
            st = replaceAll(st, "<log>", cidPrefix + suffixes[i]);

            for (int j = 0; j < config->nExecutions; j++) {
                auto start = std::chrono::steady_clock::now();
                parser->parse(st);
                auto end = std::chrono::steady_clock::now();
                diff += std::chrono::duration<double, std::milli>(end - start).count();
            }
            diff /= config->nExecutions;
            results[i].push_back(diff);
        }
    }

    writeResults(results, statements, cidPrefix, suffixes);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "Usage: csql.exe <path_to_config_file>" << std::endl;
        return -1;
    }

    auto *config = (new ConfigurationManager(argv[1]))->getConfiguration();
    int testing = 0;

    if (argc > 2) {
        if (!strcmp(argv[2], "-g")) {
            // Generate events
            testing = generateEvents(config, argc, argv);
        } else if (!strcmp(argv[2], "-i")) {
            // Bring indexes to memory
            for (int i = 3; i < argc; i++) {
                if (!strcmp(argv[i], "-t")) {
                    testing = 1;
                    break;
                }
                std::string buf(argv[i]);
                MemoryIndex::generate(buf);
            }
        } else if (!strcmp(argv[2], "-t")) {
            return executeTests(config);
        } else {
            std::cout << "Invalid arguments" << std::endl;
            return -1;
        }

        return testing ? executeTests(config) : mainLoop(config);
    }

    return mainLoop(config);
}
