//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_EVENTREADER_H
#define ChronoSQL_EVENTREADER_H


#include <fstream>
#include <string>

class EventReader {

public:

    virtual const char *readLastEvent(const CID &cid) { return nullptr; }

    virtual std::list<std::pair<EID, const char *>> *
    readEventsInRange(const CID &cid, std::time_t start, std::time_t end) { return {}; }

protected:

    static std::ifstream openReadFile(const std::string &filename) {
        std::ifstream input_file;
        input_file.open(filename, std::ios::ate);
        return input_file;
    }
};


#endif //ChronoSQL_EVENTREADER_H
