//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_INDEXEDEVENTREADER_H
#define CHRONOSQL_INDEXEDEVENTREADER_H


#include "EventReader.h"

class IndexedEventReader : public EventReader {

public:
    explicit IndexedEventReader(int _fixedPayloadSize) : fixedPayloadSize(_fixedPayloadSize) {}

private:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_INDEXEDEVENTREADER_H
