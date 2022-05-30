//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITERFACTORY_H
#define CHRONOSQL_POC_EVENTWRITERFACTORY_H


#include "EventWriter.h"
#include "../Config/ConfigurationManager.h"
#include "FSKeyValueEventWriterFactory.h"
#include "IndexedKeyValueEventWriterFactory.h"

class EventWriterFactory {
public:
    [[nodiscard]] EventWriter *getWriter(const ConfigurationValues *config) {
        if (config->eventType == EventType::FIXED_KEY_VALUE) {
            auto *kvWriterFactory = new FSKeyValueEventWriterFactory(config);
            return kvWriterFactory->getWriter();
        } else if (config->eventType == EventType::INDEXED_KEY_VALUE) {
            auto *indexedWriterFactory = new IndexedKeyValueEventWriterFactory(config);
            return indexedWriterFactory->getWriter();
        }

        return nullptr;
    };
};


#endif //CHRONOSQL_POC_EVENTWRITERFACTORY_H
