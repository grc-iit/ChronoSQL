//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITERFACTORY_H
#define CHRONOSQL_POC_EVENTWRITERFACTORY_H

#include "FSMemoryKeyValueEventWriterFactory.h"
#include "FSKeyValueEventWriterFactory.h"

class EventWriterFactory {
public:
    EventWriter *getWriter(const ConfigurationValues *config) {
        if (config->eventType == EventType::MEMORY_KEY_VALUE) {
            auto *memFactory = new FSMemoryKeyValueEventWriterFactory(config);
            return memFactory->getWriter();
        } else if (config->eventType == EventType::FIXED_KEY_VALUE) {
            auto *fixedFactory = new FSKeyValueEventWriterFactory(config);
            return fixedFactory->getWriter();
        }

        return nullptr;
    }
};


#endif //CHRONOSQL_POC_EVENTWRITERFACTORY_H
