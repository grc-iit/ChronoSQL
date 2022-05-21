//
// Created by pablo on 21/05/2022.
//

#ifndef CHRONOSQL_POC_ENUMERATIONS_H
#define CHRONOSQL_POC_ENUMERATIONS_H

#include <unordered_map>

namespace Enumerations {

    // Enum to determine the type of events that will be generated
    enum class EventType {
        KEY_VALUE, TEST
    };

    // Map to associate string values to the enum EventType
    static std::unordered_map<std::string, EventType> const EventTypeValues =
            {{"KEY_VALUE", EventType::KEY_VALUE},
             {"TEST",      EventType::TEST}};
}


#endif //CHRONOSQL_POC_ENUMERATIONS_H
