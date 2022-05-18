//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
#define CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H


#include "EventGenerator.h"
#include "../Event/KeyValueEvent.h"

class KeyValueEventGenerator : public EventGenerator {

public:
    explicit KeyValueEventGenerator(int payloadSize_) : payloadSize(payloadSize_) {}

    [[nodiscard]] Event *generateEvent() const override {
        auto *event = new KeyValueEvent(std::time(nullptr), generateRandomBytes(payloadSize));

        return event;
    }

private:
    int payloadSize;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
