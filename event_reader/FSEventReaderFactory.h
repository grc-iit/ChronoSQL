//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_FSEVENTREADERFACTORY_H
#define ChronoSQL_FSEVENTREADERFACTORY_H


#include <string>
#include <utility>
#include "EventReader.h"
#include "FSEventReader.h"
#include "../common/Constants.h"
#include "../config/ConfigurationValues.h"

using namespace Constants;

class FSEventReaderFactory {

public:
    FSEventReaderFactory(const ConfigurationValues *config) : fixedPayloadSize(config->fixedPayloadSize) {}

    [[nodiscard]] EventReader *getReader() const {
        return new FSEventReader(fixedPayloadSize);
    }

private:
    int fixedPayloadSize;
};


#endif //ChronoSQL_FSEVENTREADERFACTORY_H
