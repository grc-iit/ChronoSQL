//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTGENERATOR_H
#define CHRONOSQL_POC_EVENTGENERATOR_H

#include <cstring>
#include <random>
#include "../Event/Event.h"

class EventGenerator {
public:

    virtual Event *generateEvent() const { return nullptr; };

    char *generateRandomChar(int size) const {

        char *result = new char[size];
        std::mt19937 rg{std::random_device{}()};
        std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(charset) - 2);

        for (int i = 0; i < size; i++) {
            result[i] = 'a' + charset[pick(rg)];
        }

        return result;
    }

protected:
    const char charset[63] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
};

#endif //CHRONOSQL_POC_EVENTGENERATOR_H
