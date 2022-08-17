//
// Created by pablo on 17/05/2022.
//

#ifndef ChronoSQL_KEYVALUEEVENTGENERATOR_H
#define ChronoSQL_KEYVALUEEVENTGENERATOR_H


#include "EventGenerator.h"
#include "../event/KeyValueEvent.h"

class KeyValueEventGenerator : public EventGenerator {

public:
    explicit KeyValueEventGenerator(int _payloadSize, int _payloadVariation, EID _lowerTimestamp, EID _higherTimestamp)
            : payloadSize(_payloadSize), payloadVariation(_payloadVariation), lowerTimestamp(_lowerTimestamp),
              higherTimestamp(_higherTimestamp) {
    }

    [[nodiscard]] Event *generateEvent() const override {
        return generateEvent(payloadSize);
    }

    [[nodiscard]] Event *generateEvent(int size) const override {
        return new KeyValueEvent(generateRandomEID(), generateRandomBytes(size));
    }

    [[nodiscard]] std::list<Event *> generateEvents(int nEvents) const override {
        std::list<Event *> events;

        // A random value between payloadSize +/- payloadVariation will be generated for each event
        std::mt19937 rg{std::random_device{}()};
        std::uniform_int_distribution<long> pick(lowerTimestamp, higherTimestamp);

        for (int i = 0; i < nEvents; i++) {
            events.push_back(new KeyValueEvent(pick(rg), generateRandomBytes(payloadSize)));
        }
        return events;
    }

    [[nodiscard]] EID generateRandomEID() const {
        std::mt19937 rg{std::random_device{}()};
        std::uniform_int_distribution<long> pick(lowerTimestamp, higherTimestamp);
        return pick(rg);
    }

private:
    int payloadSize;
    int payloadVariation;
    EID lowerTimestamp;
    EID higherTimestamp;
};


#endif //ChronoSQL_KEYVALUEEVENTGENERATOR_H
