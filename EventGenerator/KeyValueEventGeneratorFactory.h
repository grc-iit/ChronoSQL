//
// Created by pablo on 18/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H
#define CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H


#include "EventGeneratorFactory.h"
#include "KeyValueEventGenerator.h"

class KeyValueEventGeneratorFactory : public EventGeneratorFactory {
public:
    explicit KeyValueEventGeneratorFactory(int payloadSize_, int payloadVariation_) :
            payloadSize(payloadSize_), payloadVariation(payloadVariation_) {}

    [[nodiscard]] EventGenerator *getGenerator() const override {
        return new KeyValueEventGenerator(payloadSize, payloadVariation);
    }

private:
    int payloadSize;
    int payloadVariation;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTGENERATORFACTORY_H
