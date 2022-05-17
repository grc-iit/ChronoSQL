//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITER_H


#include <iostream>
#include "EventWriter.h"

class KeyValueEventWriter : public EventWriter {
public:
    int writeToFile(std::string filename, std::string payload, std::string args[]) const override {
        if (args->length() > 0) {
            std::ofstream output_file = KeyValueEventWriter::openFile(filename);
            output_file << args[0] << ", " << payload << std::endl;
            output_file.close();
            std::cout << "Hey!";

            return 0;
        }

        return 1;
    }
};

#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
