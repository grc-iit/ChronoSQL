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
        storage = new MemoryEventStorage();
    }

    char *readLastEvent() override {
        return (dynamic_cast<KeyValueEvent *>(storage->getEvents()->back()))->getPayload();
    }

    std::list<char *> readEventsInRange(std::time_t start, std::time_t end) override {
        std::list<char *> eventsInRange;
        for (auto event: *storage->getEvents()) {
            auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
            if ((start == VOID_TIMESTAMP || kvEvent->getTimestamp() >= start) &&
                (end == VOID_TIMESTAMP || kvEvent->getTimestamp() <= end)) {
                eventsInRange.push_back(kvEvent->getPayload());
            }

            if (end != VOID_TIMESTAMP && kvEvent->getTimestamp() > end) {
                break;
            }
        }

        return eventsInRange;
    }

private:
//    std::list<Event *> *events;
    MemoryEventStorage *storage;
};


#endif //CHRONOSQL_POC_MEMEVENTREADER_H
