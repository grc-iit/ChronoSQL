//
// Created by pablo on 18/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H
#define CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H


#include "EventGeneratorFactory.h"
#include "KeyValueEventGenerator.h"
#include "../Config/ConfigurationManager.h"

class KeyValueEventGeneratorFactory {
public:
    explicit KeyValueEventGeneratorFactory(const ConfigurationManager *config) :
            payloadSize(config->payloadSize), payloadVariation(config->payloadVariation) {}

    [[nodiscard]] EventGenerator *getGenerator() const {
        return new KeyValueEventGenerator(payloadSize, payloadVariation);
    }

private:
    int payloadSize;
    int payloadVariation;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H
