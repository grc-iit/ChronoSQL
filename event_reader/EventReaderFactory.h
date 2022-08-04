//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_EVENTREADERFACTORY_H
#define ChronoSQL_EVENTREADERFACTORY_H


#include "EventReader.h"
#include "../config/ConfigurationValues.h"
#include "FSEventReaderFactory.h"
#include "MemEventReaderFactory.h"
#include "IndexedEventReaderFactory.h"

class EventReaderFactory {

public:
    EventReader *getReader(const ConfigurationValues *config) {
        if (config->eventType == EventType::FIXED_KEY_VALUE || config->eventType == EventType::INDEXED_KEY_VALUE) {
            auto *factory = new FSEventReaderFactory(config);
            return factory->getReader();
        } else if (config->eventType == EventType::MEMORY_KEY_VALUE) {
            auto *memFactory = new MemEventReaderFactory(config);
            return memFactory->getReader();
        } else if (config->eventType == EventType::INDEXED_KEY_VALUE) {
            auto *indexedFactory = new IndexedEventReaderFactory(config);
            return indexedFactory->getReader();
        } else {
            return nullptr;
        }
    }
};


#endif //ChronoSQL_EVENTREADERFACTORY_H
