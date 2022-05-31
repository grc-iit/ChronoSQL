//
// Created by pablo on 31/05/2022.
//

#ifndef CHRONOSQL_POC_MEMEVENTREADER_H
#define CHRONOSQL_POC_MEMEVENTREADER_H


#include <list>
#include "EventReader.h"
#include "../event/Event.h"
#include "../event/KeyValueEvent.h"

class MemEventReader : public EventReader {

public:
    explicit MemEventReader(std::string logfile_) {
        logfile = std::move(logfile_);
    }

    void setEvents(std::list<Event *> *events_) {
        events = events_;
    }

    char *readLastEvent() override {
        Event *last = events->back();

        return (dynamic_cast<KeyValueEvent *>(last))->getPayload();
    }

private:
    std::list<Event *> *events;
};


#endif //CHRONOSQL_POC_MEMEVENTREADER_H
