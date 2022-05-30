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

        std::ifstream file = openReadFile(logfile);
        std::streampos fileSize = file.tellg();
        std::list<char *> events;

        int i = 0;
        // Size = payload + 10 (EID) + comma + semicolon
        int readSize = fixedPayloadSize + 10 + 1 + 1;
        while (i + readSize <= fileSize) {
            char *data = new char[fixedPayloadSize + 1];
            file.seekg(i + 10 + 1);
            file.get(data, fixedPayloadSize + 1);
            events.push_back(data);
            i += readSize;
        }

        return events;
    }

//    void initOffset() {
//        std::ifstream file = openReadFile(eventFile);
//        std::streampos fileSize = file.tellg();
//
//        // 1 char for '\0', fixedPayloadSize chars for payload, 1 char for ',', 10 chars for timestamp, 1 char for '@'
//        int offsetPosition = fixedPayloadSize + 10 + 3 + 1;
//        if (fileSize >= offsetPosition) {
//            char c;
//            std::string retrievedOffset;
//            file.seekg(-offsetPosition, std::ios::end);
//            file.get(c);
//
//            while (c != '\0' && c != ';' && (offsetPosition) <= fileSize) {
//                retrievedOffset.insert(0, 1, c);
//                offsetPosition++;
//                file.seekg(-offsetPosition, std::ios::end);
//                file.get(c);
//            }
//
//            offset = std::stoi(retrievedOffset) + 1;
//        }
//    }

private:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_POC_FSEVENTREADER_H
