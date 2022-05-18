//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITER_H


#include <iostream>
#include <utility>
#include <cstring>
#include "EventWriter.h"
#include "../Event/KeyValueEvent.h"

class KeyValueEventWriter : public EventWriter {

public:
    explicit KeyValueEventWriter(std::string output_file, int fixedPayloadSize_) : fixedPayloadSize(fixedPayloadSize_) {
        m_output_file = std::move(output_file);
    }

    [[nodiscard]] int writeToFile(Event *event) const override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            std::ofstream output_file = KeyValueEventWriter::openFile(m_output_file);
            output_file << kvEvent->getTimestamp() << ',' << trimByteSequence(kvEvent->getPayload()) << ';';
            output_file.close();
            return 0;
        }

        return 1;
    }

private:
    int fixedPayloadSize;

    char *trimByteSequence(char *payload) const {
        int receivedSize = strlen(payload);

        if (receivedSize == fixedPayloadSize) {
            return payload;
        }

        char *trimmed = new char[fixedPayloadSize];

        if (receivedSize < fixedPayloadSize) {
            // Pad whitespaces at the end
            strncpy(trimmed, payload, receivedSize);
            for (int i = receivedSize - 1; i < fixedPayloadSize; i++) {
                trimmed[i] = ' ';
            }
        } else {
            strncpy(trimmed, payload, fixedPayloadSize);
        }

        return trimmed;
    }
};

#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
