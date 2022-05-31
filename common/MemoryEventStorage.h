//
// Created by pablo on 31/05/2022.
//

#ifndef CHRONOSQL_POC_MEMORYEVENTSTORAGE_H
#define CHRONOSQL_POC_MEMORYEVENTSTORAGE_H


#include <list>
#include "../event/Event.h"

class MemoryEventStorage {

public:
    static std::list<Event *> *events;

    void initialize() {
        events = new std::list<Event *>;
    }

    std::list<Event *> *getEvents() {
        return events;
    }

    void setEvents(std::list<Event *> *events_) {
        events = events_;
    }

    void addEvent(Event *event) {
        events->push_back(event);
    }

private:

};

std::list<Event *> *MemoryEventStorage::events = new std::list<Event *>;


#endif //CHRONOSQL_POC_MEMORYEVENTSTORAGE_H
