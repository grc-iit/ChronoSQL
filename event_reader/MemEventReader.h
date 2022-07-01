//
// Created by pablo on 31/05/2022.
//

#ifndef ChronoSQL_MEMEVENTREADER_H
#define ChronoSQL_MEMEVENTREADER_H


#include <list>
#include "EventReader.h"
#include "../event/Event.h"
#include "../event/KeyValueEvent.h"

class MemEventReader : public EventReader {

public:
    explicit MemEventReader() = default;

    char *readLastEvent(const CID &cid) override {
        return (dynamic_cast<KeyValueEvent *>(MemoryEventStorage::getEvents(cid)->back()))->getPayload();
    }

    std::list<char *> *readEventsInRange(const CID &cid, std::time_t start, std::time_t end) override {
        auto *eventsInRange = new std::list<char *>;
        std::list<Event *> *events = MemoryEventStorage::getEvents(cid);

        if (events == nullptr) {
            return nullptr;
        }

        for (auto event: *events) {
            auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
            if ((start == VOID_TIMESTAMP || kvEvent->getTimestamp() >= start) &&
                (end == VOID_TIMESTAMP || kvEvent->getTimestamp() <= end)) {
                eventsInRange->push_back(kvEvent->getPayload());
            }

            if (end != VOID_TIMESTAMP && kvEvent->getTimestamp() > end) {
                break;
            }
        }

        return eventsInRange;
    }
};


#endif //ChronoSQL_MEMEVENTREADER_H
