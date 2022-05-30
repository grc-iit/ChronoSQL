//
// Created by pablo on 21/05/2022.
//

#ifndef CHRONOSQL_POC_CONFIGURATIONVALUES_H
#define CHRONOSQL_POC_CONFIGURATIONVALUES_H

#include "Enumerations.h"
#include "ConfigConstants.h"

using namespace Enumerations;
using namespace ConfigConstants;

class ConfigurationValues {

public:
    // Common configuration
    int nEvents;
    EventType eventType;
    std::string outputFile;

    // Key-value configuration
    int payloadSize = 0;
    int payloadVariation = 0;
    int fixedPayloadSize = 0;
};


#endif //CHRONOSQL_POC_CONFIGURATIONVALUES_H
