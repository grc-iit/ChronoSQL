//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_FSEVENTREADER_H
#define ChronoSQL_FSEVENTREADER_H


#include "EventReader.h"
#include "DiskEventReader.h"

#include <utility>
#include <iostream>

class FSEventReader : public DiskEventReader {

public:
    explicit FSEventReader(int fixedPayloadSize_) : DiskEventReader(fixedPayloadSize_) {}

    std::list<std::pair<EID, const char *>> *
    readEventsInRange(const CID &cid, std::time_t start, std::time_t end) override {
        // Very naive implementation: start reading from beginning until EID >= start is found
        // Read from that point until EID > end is found
        // Alternative: perform binary search to find an event in the range, and start from there

        std::ifstream file = openReadFile(cid + LOG_EXTENSION);
        std::streampos fileSize = file.tellg();
        auto *events = new std::list<std::pair<EID, const char *>>;

        int i = 0;
        // Size = payload + 10 (EID) + comma + semicolon
        int readSize = fixedPayloadSize + 10 + 1 + 1;
        while (i + readSize <= fileSize) {
            char *id = new char[10];
            char *data = new char[fixedPayloadSize + 1];

            file.seekg(i);
            file.get(id, 11);
            auto eid = (std::time_t) strtol(id, nullptr, 10);

            if ((start == VOID_TIMESTAMP || eid >= start) && (end == VOID_TIMESTAMP || eid <= end)) {
                file.seekg(i + 10 + 1);
                file.get(data, fixedPayloadSize + 1);
                events->push_back(std::pair(eid, data));
            }

            if (end != VOID_TIMESTAMP && eid > end) {
                break;
            }

            i += readSize;
        }

        return events;
    }
};


#endif //ChronoSQL_FSEVENTREADER_H
