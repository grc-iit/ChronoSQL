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
        return DiskEventReader::readEventsInRange(cid + LOG_EXTENSION, 0, start, end);
    }
};


#endif //ChronoSQL_FSEVENTREADER_H
