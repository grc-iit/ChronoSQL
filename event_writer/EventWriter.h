//
// Created by pablo on 16/05/2022.
//

#ifndef ChronoSQL_EVENTWRITER_H
#define ChronoSQL_EVENTWRITER_H

#include <string>
#include <fstream>
#include <list>
#include "../event/Event.h"
#include "../common/typedefs.h"

class EventWriter {

public:
    virtual int write(const CID &cid, Event *event) { return 0; };

    virtual int write(const CID &cid, std::list<Event *> events) { return {}; };

    static std::ofstream openWriteFile(const std::string &filename) {
        std::ofstream output_file;
        output_file.open(filename, std::ios::out | std::ios::app);
        return output_file;
    }
};

#endif //ChronoSQL_EVENTWRITER_H
