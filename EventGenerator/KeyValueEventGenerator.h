//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
#define CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H


#include "EventGenerator.h"
#include "../Event/KeyValueEvent.h"

class KeyValueEventGenerator : public EventGenerator {

public:
    [[nodiscard]] Event *generateEvent() const override {
        auto *event = new KeyValueEvent(std::time(nullptr), generateRandomChar(10));

        return event;
    }
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTGENERATOR_H
