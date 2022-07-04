//
// Created by pablo on 3/7/22.
//

#ifndef CHRONOSQL_CONDITIONEXPRESSION_H
#define CHRONOSQL_CONDITIONEXPRESSION_H

#include <utility>

#include "SQLParser.h"

enum class ConditionType {
    INT, DOUBLE, STRING
};

class ConditionExpression {

public:
    hsql::OperatorType operatorType;
    ConditionType type;
    std::string fieldName;
    long intValue;
    double doubleValue;
    std::string stringValue;

    ConditionExpression(hsql::OperatorType _operatorType, std::string _fieldName, long _intValue) : operatorType(
            _operatorType), fieldName(std::move(_fieldName)), intValue(_intValue) {
        type = ConditionType::INT;
    }

    ConditionExpression(hsql::OperatorType _operatorType, std::string _fieldName, double _doubleValue) : operatorType(
            _operatorType), fieldName(std::move(_fieldName)), doubleValue(_doubleValue) {
        type = ConditionType::DOUBLE;
    }

    ConditionExpression(hsql::OperatorType _operatorType, std::string _fieldName, std::string _stringValue)
            : operatorType(
            _operatorType), fieldName(std::move(_fieldName)), stringValue(std::move(_stringValue)) {
        type = ConditionType::STRING;
    }
};


#endif //CHRONOSQL_CONDITIONEXPRESSION_H
