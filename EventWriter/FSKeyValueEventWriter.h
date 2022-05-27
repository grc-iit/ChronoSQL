//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_FSKEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_FSKEYVALUEEVENTWRITER_H


#include <iostream>
#include <utility>
#include <cstring>
#include "EventWriter.h"
#include "../Event/KeyValueEvent.h"

class FSKeyValueEventWriter : public EventWriter {

public:
    explicit FSKeyValueEventWriter(std::string output_file, int fixedPayloadSize_) : fixedPayloadSize(
            fixedPayloadSize_) {
        m_output_file = std::move(output_file);
        offset = 1;
        initOffset();
    }

    int writeToFile(Event *event) override {
        KeyValueEvent *kvEvent = toKeyValue(event);
        if (kvEvent != nullptr) {
            std::ofstream outputFile = openWriteFile(m_output_file);
            writeToOutputFile(outputFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            outputFile.close();
            return 0;
        }

        return 1;
    }

    int writeToFile(std::list<Event *> events) override {
        std::ofstream outputFile = openWriteFile(m_output_file);
        for (auto const i: events) {
            KeyValueEvent *kvEvent = toKeyValue(i);
            if (kvEvent != nullptr) {
                writeToOutputFile(outputFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            }
        }
        outputFile.close();

        return 0;
    }

private:
    int fixedPayloadSize;

    static KeyValueEvent *toKeyValue(Event *event) {
        // TODO is this ok?
        return dynamic_cast<KeyValueEvent *>(event);
    }

    void initOffset() {
        std::ifstream file = openReadFile(m_output_file);
        std::streampos fileSize = file.tellg();

        // 1 char for '\0', fixedPayloadSize chars for payload, 1 char for ',', 10 chars for timestamp, 1 char for '@'
        int offsetPosition = fixedPayloadSize + 10 + 3 + 1;
        if (fileSize >= offsetPosition) {
            char c;
            std::string retrievedOffset;
            file.seekg(-offsetPosition, std::ios::end);
            file.get(c);

            while (c != '\0' && c != ';' && (offsetPosition) <= fileSize) {
                retrievedOffset.insert(0, 1, c);
                offsetPosition++;
                file.seekg(-offsetPosition, std::ios::end);
                file.get(c);
            }

            offset = std::stoi(retrievedOffset) + 1;
        }
    }

    void writeToOutputFile(std::ofstream &outFile, std::time_t timestamp, char *payload) {
        outFile << offset << '@' << timestamp << ',' << trimByteSequence(payload) << ';';
        offset++;
    }

    char *trimByteSequence(char *payload) const {
        int receivedSize = strlen(payload);

        if (receivedSize == fixedPayloadSize) {
            return payload;
        }

        char *trimmed = new char[fixedPayloadSize + 1]; // Null terminating character (+1)

        if (receivedSize < fixedPayloadSize) {
            // Copy the source sequence
            strncpy(trimmed, payload, receivedSize);
            // And pad with whitespaces at the end
            for (int i = receivedSize; i < fixedPayloadSize; i++) {
                trimmed[i] = ' ';
            }
        } else {
            // Copy only until we reach the max size
            strncpy(trimmed, payload, fixedPayloadSize);
        }
        // Terminate with the null char
        trimmed[fixedPayloadSize] = '\0';

        return trimmed;
    }
};

#endif //CHRONOSQL_POC_FSKEYVALUEEVENTWRITER_H
