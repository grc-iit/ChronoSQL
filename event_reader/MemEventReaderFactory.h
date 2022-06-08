//
// Created by pablo on 31/05/2022.
//

#ifndef CHRONOSQL_POC_MEMEVENTREADERFACTORY_H
#define CHRONOSQL_POC_MEMEVENTREADERFACTORY_H


#include "../common/Constants.h"
#include "../config/ConfigurationValues.h"
#include "EventReader.h"
#include "MemEventReader.h"

using namespace Constants;

class MemEventReaderFactory {

public:
    explicit MemEventReaderFactory(const ConfigurationValues *config) {
        logfile = config->outputFile + LOG_EXTENSION;
        reader = new MemEventReader(logfile);
    }

    [[nodiscard]] EventReader *getReader() const {
        return reader;
    }

private:
    std::string logfile;
    MemEventReader *reader;
};


#endif //CHRONOSQL_POC_MEMEVENTREADERFACTORY_H
