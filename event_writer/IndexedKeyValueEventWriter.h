//
// Created by pablo on 26/05/2022.
//

#ifndef ChronoSQL_INDEXEDKEYVALUEEVENTWRITER_H
#define ChronoSQL_INDEXEDKEYVALUEEVENTWRITER_H


#include <cstring>
#include <iostream>
#include "EventWriter.h"
#include "../common/Constants.h"
#include "../event/KeyValueEvent.h"
#include "EventWriterUtils.h"
#include "../common/MemoryIndex.h"

using namespace Constants;

class IndexedKeyValueEventWriter : public EventWriter {

public:
    explicit IndexedKeyValueEventWriter(int _fixedPayloadSize, int _indexIntervalBytes) : fixedPayloadSize(
            _fixedPayloadSize), indexIntervalBytes(_indexIntervalBytes) {
        writerUtils = new EventWriterUtils(_fixedPayloadSize);
    }

    int write(const CID &cid, Event *event) override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            std::ofstream outputFile = openWriteFile(cid + LOG_EXTENSION);
            writeToOutputFile(outputFile, kvEvent->getTimestamp(), kvEvent->getPayload());
            outputFile.close();

            updateIndex(cid, kvEvent);

            return 0;
        }

        return 1;
    }

    int write(const CID &cid, std::list<Event *> events) override {
        std::ofstream outputFile = openWriteFile(cid + LOG_EXTENSION);
        std::ofstream outputIndexFile = openWriteFile(cid + INDEX_EXTENSION);
        for (auto const i: events) {
            auto *kvEvent = dynamic_cast<KeyValueEvent *>(i);
            if (kvEvent != nullptr) {
                writeToOutputFile(outputFile, kvEvent->getTimestamp(), kvEvent->getPayload());
                updateIndex(cid, kvEvent);
            }
        }
        outputFile.close();

        return 1;
    }

private:
    const int fixedPayloadSize;
    long currentByteCount = 0;
    long totalByteCount = 0;
    long indexIntervalBytes;
    EventWriterUtils *writerUtils;

    void writeToOutputFile(std::ofstream &outFile, std::time_t timestamp, const char *payload) {
        outFile << timestamp << ',' << writerUtils->trimByteSequence(payload) << ';';
    }

    void writeToOutputFile(std::ofstream &outFile, std::time_t timestamp, long byteCount) {
        outFile << timestamp << ',' << byteCount << ';';
    }

    // Updates the index file whenever the number of unindexed bytes is greater than the threshold
    void updateIndex(const CID &cid, KeyValueEvent *kvEvent) {
        // Size equals size of EID, size of payload + one comma + one semicolon
        int eventSize = fixedPayloadSize + getNumberOfDigits(kvEvent->getTimestamp()) + 2;
        if (currentByteCount + eventSize >= indexIntervalBytes) {
            MemoryIndex::addEntry(kvEvent->getTimestamp(), totalByteCount);
            std::ofstream outputIndexFile = openWriteFile(cid + INDEX_EXTENSION);
            writeToOutputFile(outputIndexFile, kvEvent->getTimestamp(), totalByteCount);
            outputIndexFile.close();
            currentByteCount = 0;
        } else {
            currentByteCount += eventSize;
        }
        totalByteCount += eventSize;
    }

    static int getNumberOfDigits(long i) {
        return i > 0 ? (int) log10((double) i) + 1 : 1;
    }
};


#endif //ChronoSQL_INDEXEDKEYVALUEEVENTWRITER_H
