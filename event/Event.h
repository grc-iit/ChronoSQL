//
// Created by pablo on 17/05/2022.
//

#ifndef ChronoSQL_EVENT_H
#define ChronoSQL_EVENT_H


#include <ctime>

class Event {

public:
    virtual ~Event() = default;

    Event() : timestamp(std::time(nullptr)) {}

    explicit Event(std::time_t timestamp_) : timestamp(timestamp_) {}

    [[nodiscard]] std::time_t getTimestamp() const {
        return timestamp;
    }

private:
    std::time_t timestamp;
};


#endif //ChronoSQL_EVENT_H
