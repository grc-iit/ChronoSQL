//
// Created by pablo on 16/05/2022.
//

#ifndef ChronoSQL_FSKEYVALUEEVENTWRITER_H
#define ChronoSQL_FSKEYVALUEEVENTWRITER_H


#include <iostream>
#include <utility>
#include <cstring>
#include "EventWriter.h"
#include "../common/Constants.h"
#include "../event/KeyValueEvent.h"

using namespace Constants;

class FSKeyValueEventWriter : public EventWriter {

public:
    explicit FSKeyValueEventWriter(int fixedPayloadSize_) : fixedPayloadSize(fixedPayloadSize_) {}

    int write(const CID &cid, Event *event) override {
        KeyValueEvent *kvEvent = toKeyValue(event);
        if (kvEvent != nullptr) {
            std::ofstream outputFile = openWriteFile(cid + LOG_EXTENSION);
            writeToOutputFile(outputFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            outputFile.close();
            return 0;
        }

        return 1;
    }

    int write(const CID &cid, std::list<Event *> events) override {
        std::ofstream outputFile = openWriteFile(cid + LOG_EXTENSION);
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

    void writeToOutputFile(std::ofstream &outFile, std::time_t timestamp, char *payload) {
        outFile << timestamp << ',' << trimByteSequence(payload) << ';';
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

#endif //ChronoSQL_FSKEYVALUEEVENTWRITER_H
