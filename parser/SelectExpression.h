//
// Created by pablo on 1/7/22.
//

#ifndef CHRONOSQL_SELECTEXPRESSION_H
#define CHRONOSQL_SELECTEXPRESSION_H


#include <string>
#include <list>
#include <utility>

class SelectExpression {

public:
    bool isStar;
    bool isFunction;
    bool isColumn;
    std::string name;
    std::list<SelectExpression *> nestedExpressions;

    static SelectExpression *starExpression() {
        auto *expr = new SelectExpression();
        expr->isStar = true;

        return expr;
    }

    static SelectExpression *functionExpression(std::string name) {
        auto *expr = new SelectExpression();
        expr->isFunction = true;
        expr->name = std::move(name);
        expr->nestedExpressions = {};

        return expr;
    }

    static SelectExpression *columnExpression(std::string name) {
        auto *expr = new SelectExpression();
        expr->isColumn = true;
        expr->name = std::move(name);
        return expr;
    }
};


#endif //CHRONOSQL_SELECTEXPRESSION_H
