//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_DISKEVENTREADER_H
#define CHRONOSQL_DISKEVENTREADER_H


#include "EventReader.h"

class DiskEventReader : public EventReader {

public:
    explicit DiskEventReader(int _fixedPayloadSize) : fixedPayloadSize(_fixedPayloadSize) {}

    const char *readLastEvent(const CID &cid) override {
        std::ifstream file = openReadFile(cid + LOG_EXTENSION);
        int fileSize = (int) file.tellg();

        if (fileSize > fixedPayloadSize) {
            char *data = new char[fixedPayloadSize + 1];
            file.seekg(fileSize - fixedPayloadSize - 1);
            file.get(data, fixedPayloadSize + 1);
            return data;
        }

        return nullptr;
    }

    std::list<std::pair<EID, const char *>> *
    readEventsInRange(const std::string &filename, long pos, std::time_t start, std::time_t end) {
        // Very naive implementation: start reading from beginning until EID >= start is found
        // Read from that point until EID > end is found
        // Alternative: perform binary search to find an event in the range, and start from there
        std::ifstream file = openReadFile(filename);

        // Size = payload + 10 (EID) + comma + semicolon
        int readSize = fixedPayloadSize + 10 + 1 + 1;
        std::streampos fileSize = file.tellg();

        auto *events = new std::list<std::pair<EID, const char *>>;

        while (pos + readSize <= fileSize) {
            char *id = new char[10];
            char *data = new char[fixedPayloadSize + 1];

            file.seekg(pos);
            file.get(id, 11);
            auto eid = (std::time_t) strtol(id, nullptr, 10);

            if ((start == VOID_TIMESTAMP || eid >= start) && (end == VOID_TIMESTAMP || eid <= end)) {
                file.seekg(pos + 10 + 1);
                file.get(data, fixedPayloadSize + 1);
                events->push_back(std::pair(eid, data));
            }

            if (end != VOID_TIMESTAMP && eid > end) {
                break;
            }

            pos += readSize;
        }

        return events;
    }

protected:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_DISKEVENTREADER_H
