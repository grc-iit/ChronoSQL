//
// Created by pablo on 31/05/2022.
//

#ifndef ChronoSQL_MEMORYEVENTSTORAGE_H
#define ChronoSQL_MEMORYEVENTSTORAGE_H


#include <list>
#include <map>
#include "../event/Event.h"
#include "../exception/ChronicleNotFoundException.h"

class MemoryEventStorage {

public:
    static std::map<CID, std::list<Event *> *> events;

    static void initialize() {
//        events = new std::list<Event *>;
    }

    static std::list<Event *> *getEvents(const CID &cid) {
        auto pos = events.find(cid);
        if (pos == events.end()) {
            // Not found
            throw ChronicleNotFoundException();
        } else {
            return pos->second;
        }
    }

    static void addEvent(const CID &cid, Event *event) {
        auto pos = events.find(cid);
        if (pos == events.end()) {
            events.insert({cid, new std::list<Event *>});
        }

        events.at(cid)->push_back(event);
    }

    static void dumpContents() {
        std::cout << std::endl << "***** Dumping contents of the in-memory event storage... *****" << std::endl;
        for (const auto &chronicle: events) {
            std::cout << "Chronicle " << chronicle.first << std::endl;
            for (auto event: *chronicle.second) {
                if (auto *kvEvent = dynamic_cast<KeyValueEvent *>(event)) {
                    std::cout << kvEvent->getTimestamp() << ", " << kvEvent->getPayload() << std::endl;
                }
            }
        }
        std::cout << "***** Contents successfully dumped *****" << std::endl << std::endl;
    }
};

std::map<CID, std::list<Event *> *> MemoryEventStorage::events;

#endif //ChronoSQL_MEMORYEVENTSTORAGE_H
