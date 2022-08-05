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
#include "EventWriterUtils.h"

using namespace Constants;

class FSKeyValueEventWriter : public EventWriter {

public:
    explicit FSKeyValueEventWriter(int _fixedPayloadSize) : fixedPayloadSize(_fixedPayloadSize) {
        writerUtils = new EventWriterUtils(_fixedPayloadSize);
    }

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
    const int fixedPayloadSize;
    EventWriterUtils *writerUtils;

    static KeyValueEvent *toKeyValue(Event *event) {
        return dynamic_cast<KeyValueEvent *>(event);
    }

    void writeToOutputFile(std::ofstream &outFile, std::time_t timestamp, const char *payload) {
        outFile << timestamp << ',' << writerUtils->trimByteSequence(payload) << ';';
    }
};

#endif //ChronoSQL_FSKEYVALUEEVENTWRITER_H
