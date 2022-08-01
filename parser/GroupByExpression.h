//
// Created by pablo on 28/7/22.
//

#ifndef CHRONOSQL_GROUPBYEXPRESSION_H
#define CHRONOSQL_GROUPBYEXPRESSION_H


#include <string>
#include "SQLParser.h"
#include "SelectExpression.h"

class GroupByExpression {

public:
    std::string name;
    std::string alias;
    SelectExpression *expression;

    GroupByExpression(std::string _name, std::string _alias) : name(std::move(_name)), alias(std::move(_alias)) {}

};


#endif //CHRONOSQL_GROUPBYEXPRESSION_H
