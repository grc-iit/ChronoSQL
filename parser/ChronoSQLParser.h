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
#include "../exception/InvalidAggregationException.h"
#include "GroupByExpression.h"
#include "EventInterval.h"
#include "../exception/UnrecognizedDayOfTheWeek.h"

#ifndef CHRONOSQL_SQLPARSER_H
#define CHRONOSQL_SQLPARSER_H


class ChronoSQLParser {
public:

    ChronoSQLParser(ConfigurationValues *config) {
        chronoLog = new ChronoLog(config);
        hideOutput = config->hideOutput;
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
            std::cout << "Error parsing statement: " << result.errorMsg() << std::endl;
        }
    }

private:
    bool hideOutput;
    ChronoLog *chronoLog;
    static std::unordered_map<hsql::DatetimeField, long> intervalConversions;
    static std::unordered_map<std::string, Enumerations::DayOfTheWeek> daysOfTheWeek;

    const std::set<std::string> SUPPORTED_FUNCTIONS = {"COUNT", "WINDOW"};

    void printResults(std::list<std::pair<EID, const char *>> *events,
                      std::unordered_map<std::string, std::string> aliases, std::list<GroupByExpression *> *groupBy) {
        if (!hideOutput) {
            int i = 0, isAggregate = 0, isWindow = 0;
            std::cout << std::endl;

            if (events->size() > 0 &&
                (SUPPORTED_FUNCTIONS.count(events->front().second) ||
                 SUPPORTED_FUNCTIONS.count(aliases[events->front().second]))) {
                isAggregate = 1;
                isWindow =
                        (strcmp(events->front().second, "WINDOW") == 0) || aliases[events->front().second] == "WINDOW";

                std::cout << events->front().second << std::endl;

                events->pop_front();
            }

            std::cout << "----------" << std::endl;
            for (auto &event: *events) {
                std::string windowValue = isWindow ? std::to_string(event.first) + "     " : "";
                std::cout << windowValue << event.second << std::endl;
                i++;
            }

            if (!isAggregate) {
                std::cout << "(" << i << " events)" << std::endl;
            }
        }
        free(events);
    }

    int parseSelectStatement(const hsql::SelectStatement *statement) {
        auto *expressions = new std::vector<SelectExpression *>;
        std::unordered_map<std::string, std::string> aliases;

        for (hsql::Expr *expr: *statement->selectList) {
            SelectExpression *e = parseSelectToken(expr, aliases);
            if (e == nullptr) {
                return -1;
            }

            expressions->push_back(e);
        }

        std::list<ConditionExpression *> *conditions = {};
        if (statement->whereClause != nullptr) {
            conditions = parseWhereExpression(statement->whereClause);
        }

        std::list<GroupByExpression *> *groupByExpressions = {};
        if (statement->groupBy != nullptr) {
            groupByExpressions = parseGroupBy(statement->groupBy, aliases);
        }

        std::list<std::pair<EID, const char *>> *results;

        try {
            results = executeExpressions(statement->fromTable->name, expressions, conditions, *groupByExpressions,
                                         aliases);
        } catch (ChronicleNotFoundException &e) {
            std::cout << "ERROR: Chronicle \"" << statement->fromTable->name << "\" does not exist" << std::endl;
            return -1;
        } catch (FieldNotFoundException &e) {
            std::cout << "ERROR: Field \"" << e.getField() << "\" does not exist" << std::endl;
            return -1;
        } catch (InvalidWindowArgumentException &e) {
            printException(e);
            return -1;
        } catch (InvalidAggregationException &e) {
            printException(e);
            return -1;
        } catch (UnrecognizedDayOfTheWeek &e) {
            printException(e);
            return -1;
        }

        printResults(results, aliases, groupByExpressions);

        return 0;
    }

    std::list<std::pair<EID, const char *>> *
    executeExpressions(const CID &cid, std::vector<SelectExpression *> *expressions,
                       const std::list<ConditionExpression *> *conditions,
                       std::list<GroupByExpression *> &groupBy,
                       std::unordered_map<std::string, std::string> &aliases) {
        for (SelectExpression *e: *expressions) {
            if (e->isStar) {
                auto interval = extractInterval(conditions);
                if (interval->days.empty()) {
                    return chronoLog->replay(cid, interval->startEID, interval->endEID);
                } else {
                    auto events = chronoLog->replay(cid, interval->startEID, interval->endEID);
                    auto *value = new std::list<std::pair<EID, const char *>>();
                    for (auto ev: *events) {
                        if (eventMeetsDaysOfTheWeek(ev, interval->days)) {
                            value->push_back(ev);
                        }
                    }
                    return value;
                }
            } else if (e->isFunction) {
                auto *value = new std::list<std::pair<EID, const char *>>();
                std::transform(e->name.begin(), e->name.end(), e->name.begin(), ::toupper);
                if (SUPPORTED_FUNCTIONS.count(e->name)) {
                    if (e->isAliased) {
                        value->push_back(std::pair(0, e->alias.c_str()));
                        aliases[e->alias] = e->name;    // Uppercase transformation
                    } else {
                        value->push_back(std::pair(0, e->name.c_str()));
                    }

                    if (e->name == "COUNT") {
                        auto results = executeExpressions(cid, e->nestedExpressions, conditions, groupBy, aliases);
                        value->push_back(std::pair(0, std::to_string(results->size()).c_str()));
                    } else if (e->name == "WINDOW") {
                        if (e->nestedExpressions == nullptr || e->nestedExpressions->empty() ||
                            e->nestedExpressions->front()->type != hsql::kExprLiteralInterval) {
                            throw InvalidWindowArgumentException();
                        }

                        if (!groupBy.empty()) {
                            std::transform(groupBy.front()->name.begin(), groupBy.front()->name.end(),
                                           groupBy.front()->name.begin(), ::toupper);

                            if (!groupBy.empty() && groupBy.front()->name == "WINDOW") {
                                groupBy.front()->expression = SelectExpression::intervalExpression(
                                        e->nestedExpressions->front()->value, e->nestedExpressions->front()->dateTime);
                            }

                            if (expressions->size() > 1 && expressions->at(1)->isFunction) {
                                executeWindow(cid, conditions, groupBy, *value, expressions->at(1));
                            } else {
                                executeWindow(cid, conditions, groupBy, *value, nullptr);
                            }

                        }
                    }
                    return value;
                } else {
                    throw InvalidAggregationException();
                }
            } else {
                // Handle logic
            }
        }

        return {};
    }

    static EventInterval *extractInterval(const std::list<ConditionExpression *> *conditions) {
        auto interval = new EventInterval();

        if (conditions != nullptr && !conditions->empty()) {
            for (auto cond: *conditions) {
                if (cond->fieldName == "EID") {
                    if (cond->operatorType == hsql::kOpGreater) {
                        interval->startEID = cond->intValue + 1;
                    } else if (cond->operatorType == hsql::kOpGreaterEq) {
                        interval->startEID = cond->intValue;
                    } else if (cond->operatorType == hsql::kOpLess) {
                        interval->endEID = cond->intValue - 1;
                    } else if (cond->operatorType == hsql::kOpLessEq) {
                        interval->endEID = cond->intValue;
                    } else if (cond->operatorType == hsql::kOpEquals) {
                        if (cond->stringValue.empty()) {
                            interval->startEID = cond->intValue;
                            interval->endEID = cond->intValue;
                        } else {
                            std::transform(cond->stringValue.begin(), cond->stringValue.end(),
                                           cond->stringValue.begin(), ::toupper);
                            if (daysOfTheWeek.count(cond->stringValue)) {
                                interval->days.push_back(daysOfTheWeek[cond->stringValue]);
                            } else {
                                throw UnrecognizedDayOfTheWeek();
                            }
                        }
                    }
                } else {
                    throw FieldNotFoundException(cond->fieldName);
                }
            }
        }
        return interval;
    }

    static long getIntervalSeconds(SelectExpression *interval) {
        return interval->value * intervalConversions[interval->dateTime];
    }

    void executeWindow(const CID &cid, const std::list<ConditionExpression *> *conditions,
                       std::list<GroupByExpression *> &groupBy, std::list<std::pair<EID, const char *>> &value,
                       SelectExpression *aggregate) {
        auto interval = extractInterval(conditions);
        auto events = chronoLog->replay(cid, interval->startEID, interval->endEID);

        if (!events->empty()) {
            long intervalSize = getIntervalSeconds(groupBy.front()->expression);
            long currentAgg = 0;
            EID intervalStart = events->front().first;
            EID intervalEnd = intervalStart + intervalSize;

            for (auto ev: *events) {
                if (!eventMeetsDaysOfTheWeek(ev, interval->days)) {
                    continue;
                }

                if (ev.first >= intervalEnd) {
                    if (aggregate != nullptr) {
                        value.push_back({intervalStart, longToChar(currentAgg)});
                        currentAgg = 0;
                    }

                    intervalStart = intervalEnd + trunc((ev.first - intervalEnd) / intervalSize) * intervalSize;
                    intervalEnd = intervalStart + intervalSize;
                }

                if (aggregate == nullptr) {
                    value.push_back({intervalStart, ev.second});
                } else {
                    currentAgg++;
                }
            }

            if (aggregate != nullptr) {
                value.push_back({intervalStart, longToChar(currentAgg)});
            }
        }

        free(events);
    }

    static bool
    eventMeetsDaysOfTheWeek(std::pair<EID, const char *> ev, const std::list<Enumerations::DayOfTheWeek> &dows) {
        if (dows.empty()) {
            return true;
        }

        for (Enumerations::DayOfTheWeek dow: dows) {
            if (extractDayOfTheWeek(ev.first) == dow) {
                return true;
            }
        }
        return false;
    }

    static Enumerations::DayOfTheWeek extractDayOfTheWeek(EID eid) {
        // Assumption: day of the week is in UTC
        // January 1, 1970, 00.00 was Thursday, 86400 seconds in a day
        int dow = (static_cast<long>(floor(eid / 86400)) + 4) % 7;
        return (Enumerations::DayOfTheWeek) dow;
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

    SelectExpression *parseSelectToken(hsql::Expr *expr, std::unordered_map<std::string, std::string> &aliases) {
        if (expr->type == hsql::kExprStar) {
            return SelectExpression::starExpression();
        } else if (expr->type == hsql::kExprColumnRef) {
            std::cout << " field " << expr->name;
            return SelectExpression::columnExpression(expr->name);
        } else if (expr->type == hsql::kExprFunctionRef) {
            auto *function = SelectExpression::functionExpression(expr->name, expr->alias);

            if (expr->hasAlias()) {
                aliases[expr->alias] = expr->name;
            }

            for (hsql::Expr *e: *expr->exprList) {
                // Do the same for each inner expression
                function->nestedExpressions->push_back(parseSelectToken(e, aliases));
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

    static std::list<GroupByExpression *> *
    parseGroupBy(hsql::GroupByDescription *groupBy, std::unordered_map<std::string, std::string> aliases) {
        auto *result = new std::list<GroupByExpression *>;
        for (hsql::Expr *expr: *groupBy->columns) {
            std::string name = expr->name;
            std::string alias;
            if (aliases.count(name)) {
                name = aliases[name];
                alias = expr->name;
            }
            result->push_back(new GroupByExpression(name, alias));
        }
        return result;
    }

    static void printException(std::exception &e) {
        std::cout << e.what() << std::endl;
    }

    static const char *longToChar(long value) {
        int charsRequired = snprintf(nullptr, 0, "%ld", value) + 1;
        char *stringValue = static_cast<char *>(malloc(charsRequired));
        snprintf(stringValue, charsRequired, "%ld", value);
        return stringValue;
    }
};

std::unordered_map<hsql::DatetimeField, long> ChronoSQLParser::intervalConversions = {{hsql::kDatetimeSecond, 1},
                                                                                      {hsql::kDatetimeMinute, 60},
                                                                                      {hsql::kDatetimeHour,   3600},
                                                                                      {hsql::kDatetimeDay,    86400},
                                                                                      {hsql::kDatetimeMonth,  2626288},
                                                                                      {hsql::kDatetimeYear,   31536000}};
std::unordered_map<std::string, Enumerations::DayOfTheWeek> ChronoSQLParser::daysOfTheWeek = {
        {"SUNDAY",    DayOfTheWeek::SUNDAY},
        {"MONDAY",    DayOfTheWeek::MONDAY},
        {"TUESDAY",   DayOfTheWeek::TUESDAY},
        {"WEDNESDAY", DayOfTheWeek::WEDNESDAY},
        {"THURSDAY",  DayOfTheWeek::THURSDAY},
        {"FRIDAY",    DayOfTheWeek::FRIDAY},
        {"SATURDAY",  DayOfTheWeek::SATURDAY}};


#endif //CHRONOSQL_SQLPARSER_H
