//
// Created by pablo on 2/8/22.
//

#ifndef CHRONOSQL_EVENTINTERVAL_H
#define CHRONOSQL_EVENTINTERVAL_H

#include "list"
#include "../common/typedefs.h"
#include "../common/Constants.h"
#include "../config/Enumerations.h"

class EventInterval {
public:
    EID startEID;
    EID endEID;
    std::list<Enumerations::DayOfTheWeek> days;

    EventInterval() {
        startEID = VOID_TIMESTAMP;
        endEID = VOID_TIMESTAMP;
        days = std::list<Enumerations::DayOfTheWeek>();
    }
};


#endif //CHRONOSQL_EVENTINTERVAL_H
