//
// Created by pablo on 21/05/2022.
//

#ifndef ChronoSQL_ENUMERATIONS_H
#define ChronoSQL_ENUMERATIONS_H

#include <unordered_map>

namespace Enumerations {

    // Enum to determine the type of events that will be generated
    enum class EventType {
        FIXED_KEY_VALUE, MEMORY_KEY_VALUE, INDEXED_KEY_VALUE
    };

    enum class DayOfTheWeek {
        SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY
    };

    // Map to associate string values to the enum EventType
    static std::unordered_map<std::string, EventType> const EventTypeValues =
            {{"FIXED_KEY_VALUE",   EventType::FIXED_KEY_VALUE},
             {"MEMORY_KEY_VALUE",  EventType::MEMORY_KEY_VALUE},
             {"INDEXED_KEY_VALUE", EventType::INDEXED_KEY_VALUE}};
}


#endif //ChronoSQL_ENUMERATIONS_H
