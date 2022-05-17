//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITER_H


#include <iostream>
#include <utility>
#include "EventWriter.h"

class KeyValueEventWriter : public EventWriter {
public:
    explicit KeyValueEventWriter(std::string output_file) {
        m_output_file = std::move(output_file);
    }

    int writeToFile(char *payload, std::string args[]) const override {
        if (args->length() > 0) {
            std::ofstream output_file = KeyValueEventWriter::openFile(m_output_file);
            output_file << args[0] << ", " << payload << std::endl;
            output_file.close();
            return 0;
        }

        return 1;
    }
};

#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
