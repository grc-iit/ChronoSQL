//
// Created by pablo on 18/05/2022.
//

#ifndef ChronoSQL_EVENTGENERATORFACTORY_H
#define ChronoSQL_EVENTGENERATORFACTORY_H


#include "EventGenerator.h"
#include "../Config/ConfigurationManager.h"
#include "KeyValueEventGeneratorFactory.h"
#include "KeyValueEventGenerator.h"

class EventGeneratorFactory {

public:
    [[nodiscard]] EventGenerator *getGenerator(const ConfigurationValues *config) {
        if (config->eventType == EventType::FIXED_KEY_VALUE || config->eventType == EventType::INDEXED_KEY_VALUE) {
            auto *kvGeneratorFactory = new KeyValueEventGeneratorFactory(config);
            return kvGeneratorFactory->getGenerator();
        } else {
            return nullptr;
        }

        return nullptr;
    };
};


#endif //ChronoSQL_EVENTGENERATORFACTORY_H
