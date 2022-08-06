//
// Created by pablo on 21/05/2022.
//

#ifndef ChronoSQL_CONFIGCONSTANTS_H
#define ChronoSQL_CONFIGCONSTANTS_H

#include <string>

// Configuration file members
namespace ConfigConstants {
    const char *NUMBER_EVENTS = "NUMBER_EVENTS";
    const char *EVENT_TYPE = "EVENT_TYPE";
    const char *OUTPUT_FILE = "OUTPUT_FILE";
    const char *PAYLOAD_SIZE = "PAYLOAD_SIZE";
    const char *PAYLOAD_VARIATION = "PAYLOAD_VARIATION";
    const char *FIXED_PAYLOAD_SIZE = "FIXED_PAYLOAD_SIZE";
    const char *INDEX_INTERVAL_BYTES = "INDEX_INTERVAL_BYTES";
    const char *LOWER_TIMESTAMP = "LOWER_TIMESTAMP";
    const char *HIGHER_TIMESTAMP = "HIGHER_TIMESTAMP";
}

#endif //ChronoSQL_CONFIGCONSTANTS_H
