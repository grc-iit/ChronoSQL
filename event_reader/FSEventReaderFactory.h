//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_FSEVENTREADERFACTORY_H
#define CHRONOSQL_POC_FSEVENTREADERFACTORY_H


#include <string>
#include <utility>
#include "EventReader.h"
#include "FSEventReader.h"
#include "../common/Constants.h"
#include "../config/ConfigurationValues.h"

using namespace Constants;

class FSEventReaderFactory {

public:
    FSEventReaderFactory(const ConfigurationValues *config) : fixedPayloadSize(config->fixedPayloadSize) {
        logfile = config->outputFile + LOG_EXTENSION;
    }

    [[nodiscard]] EventReader *getReader() const {
        return new FSEventReader(logfile, fixedPayloadSize);
    }

private:
    std::string logfile;
    int fixedPayloadSize;
};


#endif //CHRONOSQL_POC_FSEVENTREADERFACTORY_H
