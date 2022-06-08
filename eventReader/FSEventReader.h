//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_FSEVENTREADER_H
#define CHRONOSQL_POC_FSEVENTREADER_H


#include "EventReader.h"

#include <utility>
#include <iostream>

class FSEventReader : public EventReader {

public:
    explicit FSEventReader(std::string logfile_, int fixedPayloadSize_) : fixedPayloadSize(fixedPayloadSize_) {
        logfile = std::move(logfile_);
    }

    char *readLastEvent() override {
        std::ifstream file = openReadFile(logfile);
        int fileSize = (int) file.tellg();

        if (fileSize > fixedPayloadSize) {
            char *data = new char[fixedPayloadSize + 1];
            file.seekg(fileSize - fixedPayloadSize - 1);
            file.get(data, fixedPayloadSize + 1);
            return data;
        }

        return nullptr;
    }

    std::list<char *> readEventsInRange(std::time_t start, std::time_t end) override {
        // Very naive implementation: start reading from beginning until EID >= start is found
        // Read from that point until EID > end is found
        // Alternative: perform binary search to find an event in the range, and start from there

        std::ifstream file = openReadFile(logfile);
        std::streampos fileSize = file.tellg();
        std::list<char *> events;

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
                events.push_back(data);
            }

            if (end != VOID_TIMESTAMP && eid > end) {
                break;
            }

            i += readSize;
        }

        return events;
    }

private:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_POC_FSEVENTREADER_H
