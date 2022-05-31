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

    std::list<char *> readEventsInRange(std::time_t start, std::time_t end) override {
        std::list<char *> eventsInRange;
        for (auto event: *events) {
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
    std::list<Event *> *events;
};


#endif //CHRONOSQL_POC_MEMEVENTREADER_H
