//
// Created by pablo on 17/05/2022.
//

#ifndef ChronoSQL_KEYVALUEEVENT_H
#define ChronoSQL_KEYVALUEEVENT_H


#include "Event.h"
#include "../common/typedefs.h"

class KeyValueEvent : public Event {

public:
    explicit KeyValueEvent(const char *payload_) : Event(), payload(payload_) {}

    KeyValueEvent(EID timestamp_, const char *payload_) : Event(timestamp_), payload(payload_) {}

    const char *getPayload() {
        return payload;
    }

private:
    const char *payload;
};


#endif //ChronoSQL_KEYVALUEEVENT_H
