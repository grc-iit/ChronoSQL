//
// Created by pablo on 17/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENT_H
#define CHRONOSQL_POC_KEYVALUEEVENT_H


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


#endif //CHRONOSQL_POC_KEYVALUEEVENT_H
