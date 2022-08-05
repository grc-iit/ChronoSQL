//
// Created by pablo on 28/7/22.
//

#ifndef CHRONOSQL_INVALIDAGGREGATIONEXCEPTION_H
#define CHRONOSQL_INVALIDAGGREGATIONEXCEPTION_H

#include <exception>

class InvalidAggregationException : public std::exception {

public:
    explicit InvalidAggregationException() : std::exception() {}

    [[nodiscard]] const char *what() const noexcept override {
        return "Aggregation function is not supported. Supported aggregation functions: {COUNT, WINDOW}";
    }
};


#endif //CHRONOSQL_INVALIDAGGREGATIONEXCEPTION_H
