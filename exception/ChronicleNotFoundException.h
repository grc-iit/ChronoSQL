//
// Created by pablo on 4/7/22.
//

#ifndef CHRONOSQL_CHRONICLENOTFOUNDEXCEPTION_H
#define CHRONOSQL_CHRONICLENOTFOUNDEXCEPTION_H

#include <exception>

class ChronicleNotFoundException : public std::exception {

    std::string chronicle;

public:


    [[nodiscard]] const char *what() const noexcept override {
        return "Chronicle not found";
    }
};


#endif //CHRONOSQL_CHRONICLENOTFOUNDEXCEPTION_H
