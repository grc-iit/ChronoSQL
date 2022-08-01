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
    hsql::ExprType type;
    bool isStar;
    bool isFunction;
    bool isColumn;
    bool isAliased;
    long value;
    hsql::DatetimeField dateTime;
    std::string name;
    std::string alias;
    std::vector<SelectExpression *> *nestedExpressions;

    static SelectExpression *starExpression() {
        auto *expr = new SelectExpression();
        expr->isStar = true;
        expr->type = hsql::kExprStar;

        return expr;
    }

    static SelectExpression *functionExpression(std::string name, const char *alias) {
        auto *expr = new SelectExpression();
        expr->type = hsql::kExprFunctionRef;
        expr->isFunction = true;
        expr->name = std::move(name);
        expr->nestedExpressions = new std::vector<SelectExpression *>;

        if (alias != nullptr) {
            expr->alias = std::string(alias);
            expr->isAliased = true;
        }

        return expr;
    }

    static SelectExpression *columnExpression(std::string name) {
        auto *expr = new SelectExpression();
        expr->type = hsql::kExprColumnRef;
        expr->isColumn = true;
        expr->name = std::move(name);
        return expr;
    }

    static SelectExpression *stringExpression(std::string name) {
        auto *expr = new SelectExpression();
        expr->type = hsql::kExprLiteralString;
        expr->isColumn = false;
        expr->name = std::move(name);
        return expr;
    }

    static SelectExpression *intervalExpression(long value, hsql::DatetimeField dt) {
        auto *expr = new SelectExpression();
        expr->type = hsql::kExprLiteralInterval;
        expr->value = value;
        expr->dateTime = dt;
        return expr;
    }
};


#endif //CHRONOSQL_SELECTEXPRESSION_H
