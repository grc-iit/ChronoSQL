//
// Created by pablo on 18/05/2022.
//

#ifndef ChronoSQL_KEYVALUEEVENTGENERATORFACTORY_H
#define ChronoSQL_KEYVALUEEVENTGENERATORFACTORY_H


#include "EventGeneratorFactory.h"
#include "KeyValueEventGenerator.h"
#include "../Config/ConfigurationManager.h"

class KeyValueEventGeneratorFactory {
public:
    explicit KeyValueEventGeneratorFactory(const ConfigurationValues *config) :
            payloadSize(config->payloadSize), payloadVariation(config->payloadVariation) {}

    [[nodiscard]] EventGenerator *getGenerator() const {
        return new KeyValueEventGenerator(payloadSize, payloadVariation);
    }

private:
    int payloadSize;
    int payloadVariation;
};


#endif //ChronoSQL_KEYVALUEEVENTGENERATORFACTORY_H
