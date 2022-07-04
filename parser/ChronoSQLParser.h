//
// Created by pablo on 17/6/22.
//

#include <iostream>
#include "SQLParser.h"
#include "SelectExpression.h"
#include "ConditionExpression.h"

#ifndef CHRONOSQL_SQLPARSER_H
#define CHRONOSQL_SQLPARSER_H


class ChronoSQLParser {
public:

    ChronoSQLParser(ConfigurationValues *config) {
        chronoLog = new ChronoLog(config);
    }

    void parse(const std::string &query) {
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(query, &result);

        if (result.isValid()) {
            for (int i = 0; i < result.size(); i++) {
                const hsql::SQLStatement *statement = result.getStatement(i);
                if (statement->isType(hsql::kStmtSelect)) {
                    parseSelectStatement((const hsql::SelectStatement *) statement);
                } else {
                    std::cout << "Unsupported statement. Supported statements: {SELECT}." << std::endl;
                }
            }
        } else {
            std::cout << "Error parsing statement" << std::endl;
        }
    }

private:
    ChronoLog *chronoLog;

    void printResults(std::list<const char *> *events) {
        int i = 0;
        std::cout << std::endl;
        for (auto &event: *events) {
            std::cout << event << std::endl;
            i++;
        }
        std::cout << "(" << i << " events)" << std::endl;
    }

    int parseSelectStatement(const hsql::SelectStatement *statement) {
        std::list<SelectExpression *> expressions = {};
        std::cout << "Selecting ";

        for (hsql::Expr *expr: *statement->selectList) {
            SelectExpression *e = parseSelectToken(expr);
            if (e == nullptr) {
                return -1;
            }

            expressions.push_back(e);
        }

        std::list<ConditionExpression *> *conditions = {};
        if (statement->whereClause != nullptr) {
            conditions = parseWhereExpression(statement->whereClause);
        }

        auto results = executeExpressions(statement->fromTable->name, expressions, conditions);
        if (results == nullptr) {
            std::cout << "ERROR: Chronicle \"" << statement->fromTable->name << "\" does not exist" << std::endl;
            return -1;
        }
        printResults(results);

        std::cout << ". Selecting from " << statement->fromTable->name << std::endl;
        return 0;
    }

    std::list<const char *> *executeExpressions(const CID &cid, const std::list<SelectExpression *> &expressions,
                                                const std::list<ConditionExpression *> *conditions) {
        for (SelectExpression *e: expressions) {
            if (e->isStar) {
                EID startEID = VOID_TIMESTAMP;
                EID endEID = VOID_TIMESTAMP;
                if (conditions != nullptr && !conditions->empty())
                    for (auto cond: *conditions) {
                        if (cond->fieldName == "EID") {
                            std::cout << "conditioning!!!" << cond->intValue;
                            if (cond->operatorType == hsql::kOpGreater) {
                                startEID = cond->intValue + 1;
                            } else if (cond->operatorType == hsql::kOpGreaterEq) {
                                startEID = cond->intValue;
                            } else if (cond->operatorType == hsql::kOpLess) {
                                endEID = cond->intValue - 1;
                            } else if (cond->operatorType == hsql::kOpLess) {
                                endEID = cond->intValue;
                            } else if (cond->operatorType == hsql::kOpEquals) {
                                startEID = cond->intValue;
                                endEID = cond->intValue;
                            }
                        }
                    };
                std::cout << "Start: " << startEID << ", end: " << endEID << std::endl;
                return chronoLog->replay(cid, startEID, endEID);
            } else {
                // Handle logic
            }
        }

        return {};
    }

    std::list<ConditionExpression *> *parseWhereExpression(const hsql::Expr *expression) {

        std::string op = operatorToToken.find(expression->opType)->second;
        if (expression->opType == hsql::kOpAnd || expression->opType == hsql::kOpOr) {
            // Evaluate individual clauses
            std::list<ConditionExpression *> *expr1 = parseWhereExpression(expression->expr);
            std::list<ConditionExpression *> *expr2 = parseWhereExpression(expression->expr2);
            expr1->splice(expr1->end(), *expr2);
            return expr1;
        }

        if (expression->opType == hsql::kOpEquals || expression->opType == hsql::kOpNotEquals ||
            expression->opType == hsql::kOpLess || expression->opType == hsql::kOpGreater ||
            expression->opType == hsql::kOpLessEq || expression->opType == hsql::kOpGreaterEq) {
            auto *result = new std::list<ConditionExpression *>;
            if (expression->expr2->type == hsql::kExprLiteralInt) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr->name,
                                                (int) expression->expr2->ival));
            } else if (expression->expr2->type == hsql::kExprLiteralFloat) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr->name,
                                                expression->expr2->fval));
            } else if (expression->expr2->type == hsql::kExprLiteralString) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr->name,
                                                expression->expr2->name));
            } else if (expression->expr->type == hsql::kExprLiteralInt) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr2->name,
                                                (int) expression->ival));
            } else if (expression->expr->type == hsql::kExprLiteralFloat) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr2->name,
                                                expression->fval));
            } else if (expression->expr->type == hsql::kExprLiteralString) {
                result->push_back(
                        new ConditionExpression(expression->opType, expression->expr2->name,
                                                expression->name));
            }
            return result;
        }

        return nullptr;
    }

    SelectExpression *parseSelectToken(hsql::Expr *expr) {
        if (expr->type == hsql::kExprStar) {
            std::cout << " all(*) ";
            return SelectExpression::starExpression();
        } else if (expr->type == hsql::kExprColumnRef) {
            std::cout << " field " << expr->name;
            return SelectExpression::columnExpression(expr->name);
        } else if (expr->type == hsql::kExprFunctionRef) {
            std::cout << "function " << expr->name;
            auto *function = SelectExpression::functionExpression(expr->name);
            for (hsql::Expr *e: *expr->exprList) {
                // Do the same for each inner expression
                function->nestedExpressions.push_back(parseSelectToken(e));
            }
            return function;
        } else {
            std::cout << "Found unsupported select expression" << std::endl;
            return nullptr;
        }
    }

    const std::map<const hsql::OperatorType, const std::string> operatorToToken = {
            {hsql::kOpNone,            "None"},
            {hsql::kOpBetween,         "BETWEEN"},
            {hsql::kOpCase,            "CASE"},
            {hsql::kOpCaseListElement, "CASE LIST ELEMENT"},
            {hsql::kOpPlus,            "+"},
            {hsql::kOpMinus,           "-"},
            {hsql::kOpAsterisk,        "*"},
            {hsql::kOpSlash,           "/"},
            {hsql::kOpPercentage,      "%"},
            {hsql::kOpCaret,           "^"},
            {hsql::kOpEquals,          "="},
            {hsql::kOpNotEquals,       "!="},
            {hsql::kOpLess,            "<"},
            {hsql::kOpLessEq,          "<="},
            {hsql::kOpGreater,         ">"},
            {hsql::kOpGreaterEq,       ">="},
            {hsql::kOpLike,            "LIKE"},
            {hsql::kOpNotLike,         "NOT LIKE"},
            {hsql::kOpILike,           "ILIKE"},
            {hsql::kOpAnd,             "AND"},
            {hsql::kOpOr,              "OR"},
            {hsql::kOpIn,              "IN"},
            {hsql::kOpConcat,          "CONCAT"},
            {hsql::kOpNot,             "NOT"},
            {hsql::kOpUnaryMinus,      "-"},
            {hsql::kOpIsNull,          "IS NULL"},
            {hsql::kOpExists,          "EXISTS"}};
};


#endif //CHRONOSQL_SQLPARSER_H
