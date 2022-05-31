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

    static void initialize() {
        events = new std::list<Event *>;
    }

    static std::list<Event *> *getEvents() {
        return events;
    }

    static void setEvents(std::list<Event *> *events_) {
        events = events_;
    }

    static void addEvent(Event *event) {
        events->push_back(event);
    }
};

std::list<Event *> *MemoryEventStorage::events = new std::list<Event *>;


#endif //CHRONOSQL_POC_MEMORYEVENTSTORAGE_H
