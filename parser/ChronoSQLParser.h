//
// Created by pablo on 17/6/22.
//

#include <set>
#include <iostream>
#include <algorithm>
#include "SQLParser.h"
#include "SelectExpression.h"
#include "ConditionExpression.h"
#include "../exception/FieldNotFoundException.h"
#include "../exception/InvalidWindowArgumentException.h"

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

    const std::set<std::string> SUPPORTED_FUNCTIONS = {"COUNT", "SUM", "AVG", "WINDOW"};

    void printResults(std::list<const char *> *events) {
        int i = 0, isAggregate = 0;
        std::cout << std::endl;

        if (SUPPORTED_FUNCTIONS.count(events->front())) {
            isAggregate = 1;
            std::cout << events->front() << std::endl;
            events->pop_front();
        }

        std::cout << "--------" << std::endl;
        for (auto &event: *events) {
            std::cout << event << std::endl;
            i++;
        }

        if (!isAggregate) {
            std::cout << "(" << i << " events)" << std::endl;
        }
    }

    int parseSelectStatement(const hsql::SelectStatement *statement) {
        auto *expressions = new std::list<SelectExpression *>;

        for (hsql::Expr *expr: *statement->selectList) {
            SelectExpression *e = parseSelectToken(expr);
            if (e == nullptr) {
                return -1;
            }

            expressions->push_back(e);
        }

        std::list<ConditionExpression *> *conditions = {};
        if (statement->whereClause != nullptr) {
            conditions = parseWhereExpression(statement->whereClause);
        }

        std::list<const char *> *results;

        try {
            results = executeExpressions(statement->fromTable->name, expressions, conditions);
        } catch (ChronicleNotFoundException &e) {
            std::cout << "ERROR: Chronicle \"" << statement->fromTable->name << "\" does not exist" << std::endl;
            return -1;
        } catch (FieldNotFoundException &e) {
            std::cout << "ERROR: Field \"" << e.getField() << "\" does not exist" << std::endl;
            return -1;
        }

        printResults(results);

        return 0;
    }

    std::list<const char *> *executeExpressions(const CID &cid, std::list<SelectExpression *> *expressions,
                                                const std::list<ConditionExpression *> *conditions) {
        for (SelectExpression *e: *expressions) {
            if (e->isStar) {
                EID startEID = VOID_TIMESTAMP;
                EID endEID = VOID_TIMESTAMP;
                if (conditions != nullptr && !conditions->empty())
                    for (auto cond: *conditions) {
                        if (cond->fieldName == "EID") {
                            if (cond->operatorType == hsql::kOpGreater) {
                                startEID = cond->intValue + 1;
                            } else if (cond->operatorType == hsql::kOpGreaterEq) {
                                startEID = cond->intValue;
                            } else if (cond->operatorType == hsql::kOpLess) {
                                endEID = cond->intValue - 1;
                            } else if (cond->operatorType == hsql::kOpLessEq) {
                                endEID = cond->intValue;
                            } else if (cond->operatorType == hsql::kOpEquals) {
                                startEID = cond->intValue;
                                endEID = cond->intValue;
                            }
                        } else {
                            throw FieldNotFoundException(cond->fieldName);
                        }
                    }
                return chronoLog->replay(cid, startEID, endEID);
            } else if (e->isFunction) {
                auto *value = new std::list<const char *>();
                std::transform(e->name.begin(), e->name.end(), e->name.begin(), ::toupper);
                if (SUPPORTED_FUNCTIONS.count(e->name)) {
                    value->push_back(e->name.c_str());

                    if (e->name == "COUNT") {
                        auto results = executeExpressions(cid, e->nestedExpressions, conditions);
                        value->push_back(std::to_string(results->size()).c_str());
                    } else if (e->name == "WINDOW") {
                        if (e->nestedExpressions == nullptr || e->nestedExpressions->empty() ||
                            e->nestedExpressions->front()->type != hsql::kExprLiteralInterval) {
                            throw InvalidWindowArgumentException();
                        }

                        auto *expr = new std::list<SelectExpression *>;
                        expr->push_back(SelectExpression::starExpression());
                        auto *temp = executeExpressions(cid, expr, conditions);
                        for (auto const &v: *temp) {
                            std::string windowed("Window: " + std::string(v));
                            std::cout << windowed.c_str() << std::endl;
                            value->push_back(windowed.c_str());
                        }
                    }
//                    } else if (e->name == "SUM") {
//                        // TODO what to add here??
//                        value->push_back(std::to_string(results->size()).c_str());
//                    } else {    // AVG
//                        // TODO what to avg???
//                        double avg = 0;
////                        for ()
//                    }
                    return value;
                } else {
                    // TODO handle error
                    return {};
                }
            } else {
                // Handle logic
            }
        }

        return {};
    }

    std::list<ConditionExpression *> *parseWhereExpression(const hsql::Expr *expression) {
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
                                                (long) expression->expr2->ival));
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
                                                (long) expression->ival));
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
            return SelectExpression::starExpression();
        } else if (expr->type == hsql::kExprColumnRef) {
            std::cout << " field " << expr->name;
            return SelectExpression::columnExpression(expr->name);
        } else if (expr->type == hsql::kExprFunctionRef) {
            auto *function = SelectExpression::functionExpression(expr->name);
            for (hsql::Expr *e: *expr->exprList) {
                // Do the same for each inner expression
                function->nestedExpressions->push_back(parseSelectToken(e));
            }
            return function;
        } else if (expr->type == hsql::kExprLiteralString) {
            return SelectExpression::stringExpression(expr->name);
        } else if (expr->type == hsql::kExprLiteralInterval) {
            return SelectExpression::intervalExpression(expr->ival, expr->datetimeField);
        } else {
            std::cout << "Found unsupported select expression" << std::endl;
            return nullptr;
        }
    }
};


#endif //CHRONOSQL_SQLPARSER_H
