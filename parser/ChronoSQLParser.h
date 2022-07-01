//
// Created by pablo on 17/6/22.
//

#include <iostream>
#include "SQLParser.h"
#include "SelectExpression.h"

#ifndef CHRONOSQL_SQLPARSER_H
#define CHRONOSQL_SQLPARSER_H


class ChronoSQLParser {
public:

    ChronoSQLParser(EventReader *_eventReader) : eventReader(_eventReader) {}

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
    EventReader *eventReader;

    void printResults(std::list<char *> *events) {
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

        auto results = executeExpressions(statement->fromTable->name, expressions);
        if (results == nullptr) {
            std::cout << "ERROR: Chronicle " << statement->fromTable->name << " does not exist" << std::endl;
            return -1;
        }
        printResults(results);

        std::cout << ". Selecting from " << statement->fromTable->name << std::endl;
        return 0;
    }

    std::list<char *> *executeExpressions(const CID &cid, const std::list<SelectExpression *> &expressions) {
        for (SelectExpression *e: expressions) {
            if (e->isStar) {
                return eventReader->readEventsInRange(cid, VOID_TIMESTAMP, VOID_TIMESTAMP);
            } else {
                // Handle logic
            }
        }

        return {};
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
};


#endif //CHRONOSQL_SQLPARSER_H
