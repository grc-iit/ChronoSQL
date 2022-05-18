//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTGENERATOR_H
#define CHRONOSQL_POC_EVENTGENERATOR_H

#include <cstring>
#include <random>
#include <list>
#include "../Event/Event.h"

class EventGenerator {

public:
    [[nodiscard]] virtual Event *generateEvent() const { return nullptr; };

    [[nodiscard]] virtual Event *generateEvent(int size) const { return nullptr; };

    [[nodiscard]] virtual std::list<Event *> generateEvents(int nEvents, int payloadVariation) const { return {}; };

    [[nodiscard]] char *generateRandomBytes(int size) const {

        char *result = new char[size + 1];  // + 1 to reserve space for the null terminating char

        // Random generator
        std::mt19937 rg{std::random_device{}()};
        std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(charset) - 2);

        for (int i = 0; i < size; i++) {
            result[i] = charset[pick(rg)];
        }
        result[size] = '\0';

        return result;
    }

protected:
    const char charset[63] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
};

#endif //CHRONOSQL_POC_EVENTGENERATOR_H
