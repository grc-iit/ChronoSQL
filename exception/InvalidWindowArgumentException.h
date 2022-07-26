//
// Created by pablo on 26/7/22.
//

#ifndef CHRONOSQL_INVALIDWINDOWARGUMENT_H
#define CHRONOSQL_INVALIDWINDOWARGUMENT_H

#include <exception>

class InvalidWindowArgumentException : public std::exception {

public:
    explicit InvalidWindowArgumentException() : std::exception() {}

    [[nodiscard]] const char *what() const noexcept override {
        return "Invalid provided argument to the window function. Accepted format: <n> <date_expression>";
    }
};

#endif //CHRONOSQL_INVALIDWINDOWARGUMENT_H
