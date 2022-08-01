//
// Created by pablo on 2/8/22.
//

#ifndef CHRONOSQL_UNRECOGNIZEDDAYOFTHEWEEK_H
#define CHRONOSQL_UNRECOGNIZEDDAYOFTHEWEEK_H

#include <exception>

class UnrecognizedDayOfTheWeek : public std::exception {

public:

    explicit UnrecognizedDayOfTheWeek() : std::exception() {}

    [[nodiscard]] const char *what() const noexcept override {
        return "Equals parameter is not valid. Please, use a timestamp or a day of the week";
    }
};


#endif //CHRONOSQL_UNRECOGNIZEDDAYOFTHEWEEK_H
