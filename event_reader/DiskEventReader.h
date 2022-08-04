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

protected:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_DISKEVENTREADER_H
