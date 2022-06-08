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
#include "../eventReader/EventReader.h"
#include "../eventReader/FSEventReader.h"
#include "../eventReader/EventReaderFactory.h"

class ChronoLog {

public:
    explicit ChronoLog(ConfigurationValues *config) : configuration(config) {
        auto *writerFactory = new EventWriterFactory();
        auto *readerFactory = new EventReaderFactory();
        eventWriter = writerFactory->getWriter(config);
        eventReader = readerFactory->getReader(config);
    }

    EID record(CID cid, char *data) {
        EID id = std::time(nullptr);
        auto *event = new KeyValueEvent(id, data);
        eventWriter->write(event);
        return id;
    }

    char *playback() {
        return eventReader->readLastEvent();
    }

    std::list<char *> replay(EID startEID, EID endEID) {
        return eventReader->readEventsInRange(startEID, endEID);
    }

private:
    ConfigurationValues *configuration;
    EventWriter *eventWriter;
    EventReader *eventReader;
};


#endif //CHRONOSQL_POC_CHRONOLOG_H
