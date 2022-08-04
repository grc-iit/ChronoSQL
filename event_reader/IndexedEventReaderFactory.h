//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_INDEXEDEVENTREADERFACTORY_H
#define CHRONOSQL_INDEXEDEVENTREADERFACTORY_H


#include "../config/ConfigurationValues.h"
#include "EventReader.h"
#include "IndexedEventReader.h"

class IndexedEventReaderFactory {

public:
    IndexedEventReaderFactory(const ConfigurationValues *config) {
        this->fixedPayloadSize = config->fixedPayloadSize;
    }

    [[nodiscard]] EventReader *getReader() const {
        return new IndexedEventReader(fixedPayloadSize);
    }

private:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_INDEXEDEVENTREADERFACTORY_H
