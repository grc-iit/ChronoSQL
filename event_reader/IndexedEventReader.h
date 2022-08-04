//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_INDEXEDEVENTREADER_H
#define CHRONOSQL_INDEXEDEVENTREADER_H

#include "DiskEventReader.h"

class IndexedEventReader : public DiskEventReader {

public:
    explicit IndexedEventReader(int _fixedPayloadSize) : DiskEventReader(_fixedPayloadSize) {}

    // Find the closest event to start in the index and start the sequential scan from there until end
    std::list<std::pair<EID, const char *>> *
    readEventsInRange(const CID &cid, std::time_t start, std::time_t end) override {

        // We will start reading either from the beginning or from the closest index entry
        long i = start == VOID_TIMESTAMP ? 0 : MemoryIndex::getClosestValue(start);

        return DiskEventReader::readEventsInRange(cid + LOG_EXTENSION, i, start, end);
    }
};


#endif //CHRONOSQL_INDEXEDEVENTREADER_H
