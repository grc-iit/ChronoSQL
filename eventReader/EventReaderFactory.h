//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTREADERFACTORY_H
#define CHRONOSQL_POC_EVENTREADERFACTORY_H


#include "EventReader.h"
#include "../config/ConfigurationValues.h"
#include "FSEventReaderFactory.h"

class EventReaderFactory {

public:
    EventReader *getReader(const ConfigurationValues *config) {
        if (config->eventType == EventType::FIXED_KEY_VALUE) {
            auto *factory = new FSEventReaderFactory(config);
            return factory->getReader();
        } else {
            return nullptr;
        }
    }
};


#endif //CHRONOSQL_POC_EVENTREADERFACTORY_H
