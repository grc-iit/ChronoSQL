//
// Created by pablo on 4/7/22.
//

#ifndef CHRONOSQL_FIELDNOTFOUNDEXCEPTION_H
#define CHRONOSQL_FIELDNOTFOUNDEXCEPTION_H

#include <exception>
#include <string>
#include <utility>

class FieldNotFoundException : public std::exception {

    std::string field;

public:
    explicit FieldNotFoundException(std::string field_) : std::exception(), field(std::move(field_)) {}

    [[nodiscard]] const char *what() const noexcept override {
        return "Field not found";
    }

    std::string getField() {
        return field;
    }
};


#endif //CHRONOSQL_FIELDNOTFOUNDEXCEPTION_H
