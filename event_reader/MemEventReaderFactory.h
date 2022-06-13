//
// Created by pablo on 31/05/2022.
//

#ifndef ChronoSQL_MEMEVENTREADERFACTORY_H
#define ChronoSQL_MEMEVENTREADERFACTORY_H


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


#endif //ChronoSQL_MEMEVENTREADERFACTORY_H
