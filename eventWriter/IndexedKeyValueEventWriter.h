//
// Created by pablo on 26/05/2022.
//

#ifndef CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H


#include <cstring>
#include <iostream>
#include "EventWriter.h"
#include "../common/Constants.h"
#include "../event/KeyValueEvent.h"

using namespace Constants;

class IndexedKeyValueEventWriter : public EventWriter {

public:
    explicit IndexedKeyValueEventWriter(std::string output_file) {
        m_output_file = std::move(output_file);
        eventFile = m_output_file + '.' + LOG_EXTENSION;
        indexFile = m_output_file + '.' + index_file_extension;
    }

    int write(Event *event) override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            std::ofstream outputFile = openWriteFile(eventFile);
            std::ofstream outputIndexFile = openWriteFile(indexFile);
            writeToOutputFile(outputFile, outputIndexFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            outputFile.close();
            return 0;
        }

        return 1;
    }

    int write(std::list<Event *> events) override {
        std::ofstream outputFile = openWriteFile(eventFile);
        std::ofstream outputIndexFile = openWriteFile(indexFile);
        for (auto const i: events) {
            auto *kvEvent = dynamic_cast<KeyValueEvent *>(i);
            if (kvEvent != nullptr) {
                writeToOutputFile(outputFile, outputIndexFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            }
        }
        outputFile.close();

        return 1;
    }

private:
    const std::string index_file_extension = "index";
    std::string indexFile;

    void writeToOutputFile(std::ofstream &outFile, std::ofstream &indexOutFile, std::time_t timestamp, char *payload) {
        int payloadSize = std::strlen(payload);
        outFile.tellp();

        std::cout << "HEYYY:" << outFile.tellp() << std::endl;
    }
};


#endif //CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H
