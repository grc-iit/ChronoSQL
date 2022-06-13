//
// Created by pablo on 31/05/2022.
//

#ifndef ChronoSQL_MEMORYEVENTSTORAGE_H
#define ChronoSQL_MEMORYEVENTSTORAGE_H


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

    static void dumpContents() {
        std::cout << std::endl << "***** Dumping contents of the in-memory event storage... *****" << std::endl;
        for (auto event: *events) {
            if (auto *kvEvent = dynamic_cast<KeyValueEvent *>(event)) {
                std::cout << kvEvent->getTimestamp() << ", " << kvEvent->getPayload() << std::endl;
            }
        }
        std::cout << "***** Contents successfully dumped *****" << std::endl << std::endl;
    }
};

std::list<Event *> *MemoryEventStorage::events = new std::list<Event *>;


#endif //ChronoSQL_MEMORYEVENTSTORAGE_H
