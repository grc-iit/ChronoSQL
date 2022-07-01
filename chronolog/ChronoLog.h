//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_CHRONOLOG_H
#define ChronoSQL_CHRONOLOG_H


#include <string>
#include "../config/ConfigurationManager.h"
#include "../event_writer/EventWriter.h"
#include "../event_writer/EventWriterFactory.h"
#include "../common/typedefs.h"
#include "../event_reader/EventReader.h"
#include "../event_reader/FSEventReader.h"
#include "../event_reader/EventReaderFactory.h"

class ChronoLog {

public:
    explicit ChronoLog(ConfigurationValues *config) : configuration(config) {
        auto *writerFactory = new EventWriterFactory();
        auto *readerFactory = new EventReaderFactory();
        eventWriter = writerFactory->getWriter(config);
        eventReader = readerFactory->getReader(config);
    }

    EID record(const CID &cid, char *data) {
        EID id = std::time(nullptr);
        auto *event = new KeyValueEvent(id, data);
        eventWriter->write(cid, event);
        return id;
    }

    char *playback(const CID &cid) {
        return eventReader->readLastEvent(cid);
    }

    std::list<char *> replay(const CID &cid, EID startEID, EID endEID) {
        return eventReader->readEventsInRange(cid, startEID, endEID);
    }

private:
    ConfigurationValues *configuration;
    EventWriter *eventWriter;
    EventReader *eventReader;
};


#endif //ChronoSQL_CHRONOLOG_H
