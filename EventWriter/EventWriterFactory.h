//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITERFACTORY_H
#define CHRONOSQL_POC_EVENTWRITERFACTORY_H


#include "EventWriter.h"
#include "../Config/ConfigurationManager.h"
#include "KeyValueEventWriterFactory.h"

class EventWriterFactory {
public:
    [[nodiscard]] EventWriter *getWriter(const ConfigurationManager *config) {
        if (config->eventType == EventType::KEY_VALUE) {
            auto *kvGeneratorFactory = new KeyValueEventWriterFactory(config);
            return kvGeneratorFactory->getWriter();
        } else if (config->eventType == EventType::TEST) {
            return nullptr;
        }

        return nullptr;
    };
};


#endif //CHRONOSQL_POC_EVENTWRITERFACTORY_H
