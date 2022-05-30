//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITER_H
#define CHRONOSQL_POC_EVENTWRITER_H

#include <string>
#include <fstream>
#include <list>
#include "../event/Event.h"

class EventWriter {

public:
    virtual int writeToFile(Event *event) { return 0; };

    virtual int writeToFile(std::list<Event *> events) { return {}; };

    static std::ofstream openWriteFile(const std::string &filename) {
        std::ofstream output_file;
        output_file.open(filename, std::ios::out | std::ios::app);
        return output_file;
    }

    static std::ifstream openReadFile(const std::string &filename) {
        std::ifstream input_file;
        input_file.open(filename, std::ios::ate);
        return input_file;
    }

protected:
    std::string m_output_file;
    std::string eventFile;
    const std::string event_file_extension = "log";
};

#endif //CHRONOSQL_POC_EVENTWRITER_H
