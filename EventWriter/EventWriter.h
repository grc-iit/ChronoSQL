//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITER_H
#define CHRONOSQL_POC_EVENTWRITER_H

#include <string>
#include <fstream>
#include "../Event/Event.h"

class EventWriter {

public:
    virtual int writeToFile(Event *event) const { return 0; };

    static std::ofstream openFile(const std::string &filename) {
        std::ofstream output_file;
        output_file.open(filename, std::ios::out | std::ios::app);
        return output_file;
    }

protected:
    std::string m_output_file;
};

#endif //CHRONOSQL_POC_EVENTWRITER_H
