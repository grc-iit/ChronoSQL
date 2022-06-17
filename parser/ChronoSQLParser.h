//
// Created by pablo on 17/6/22.
//

#include <iostream>
#include "SQLParser.h"

#ifndef CHRONOSQL_SQLPARSER_H
#define CHRONOSQL_SQLPARSER_H


class ChronoSQLParser {
public:
    void parse(std::string query) {
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(query, &result);

        if (result.isValid() && result.size() > 0) {
            std::cout << "Success" << std::endl;
        } else {
            std::cout << "Invalid sql statement :(" << std::endl;
        }
    }
};


#endif //CHRONOSQL_SQLPARSER_H
