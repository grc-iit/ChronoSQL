//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_CHRONOLOG_H
#define CHRONOSQL_POC_CHRONOLOG_H


#include <string>
#include "../config/ConfigurationManager.h"
#include "../eventWriter/EventWriter.h"
#include "../eventWriter/EventWriterFactory.h"
#include "../common/typedefs.h"

class ChronoLog {

public:
    explicit ChronoLog(ConfigurationValues *config) : configuration(config) {
        auto *writerFactory = new EventWriterFactory();
        eventWriter = writerFactory->getWriter(config);
    }

    EID record(CID cid, char *data) {
        EID id = std::time(nullptr);
        auto *event = new KeyValueEvent(id, data);
        eventWriter->writeToFile(event);
        return id;
    }

private:
    ConfigurationValues *configuration;
    EventWriter *eventWriter;
};


#endif //CHRONOSQL_POC_CHRONOLOG_H
