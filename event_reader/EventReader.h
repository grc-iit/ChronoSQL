//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_EVENTREADER_H
#define ChronoSQL_EVENTREADER_H


#include <fstream>
#include <string>

class EventReader {

public:

    virtual char *readLastEvent() { return nullptr; }

    virtual std::list<char *> readEventsInRange(std::time_t start, std::time_t end) { return {}; }

protected:
    std::string logfile;

    static std::ifstream openReadFile(const std::string &filename) {
        std::ifstream input_file;
        input_file.open(filename, std::ios::ate);
        return input_file;
    }
};


#endif //ChronoSQL_EVENTREADER_H
