//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
#define CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H


#include "EventGenerator.h"
#include "../event/KeyValueEvent.h"

class KeyValueEventGenerator : public EventGenerator {

public:
    explicit KeyValueEventGenerator(int payloadSize_, int payloadVariation_) :
            payloadSize(payloadSize_), payloadVariation(payloadVariation_) {}

    [[nodiscard]] Event *generateEvent() const override {
        return generateEvent(payloadSize);
    }

    [[nodiscard]] Event *generateEvent(int size) const override {
        return new KeyValueEvent(std::time(nullptr), generateRandomBytes(size));
    }

    [[nodiscard]] std::list<Event *> generateEvents(int nEvents) const override {
        std::list<Event *> events;

        // A random value between payloadSize +/- payloadVariation will be generated for each event
        std::mt19937 rg{std::random_device{}()};
        std::uniform_int_distribution<std::string::size_type> pick(payloadSize - payloadVariation,
                                                                   payloadSize + payloadVariation);

        for (int i = 0; i < nEvents; i++) {
            events.push_back(generateEvent(pick(rg)));
        }
        return events;
    }

private:
    int payloadSize;
    int payloadVariation;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
