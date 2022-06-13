//
// Created by pablo on 17/05/2022.
//

#ifndef ChronoSQL_KEYVALUEEVENT_H
#define ChronoSQL_KEYVALUEEVENT_H


#include "Event.h"

class KeyValueEvent : public Event {

public:
    explicit KeyValueEvent(char *payload_) : Event(), payload(payload_) {}

    KeyValueEvent(std::time_t timestamp_, char *payload_) : Event(timestamp_), payload(payload_) {}

    char *getPayload() {
        return payload;
    }

private:
    char *payload;
};


#endif //ChronoSQL_KEYVALUEEVENT_H
