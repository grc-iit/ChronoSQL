//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITER_H


#include <list>
#include "../event/Event.h"
#include "../common/Constants.h"
#include "EventWriter.h"
#include "../event/KeyValueEvent.h"
#include "../common/MemoryEventStorage.h"

using namespace Constants;

class FSMemoryKeyValueEventWriter : public EventWriter {

public:
    explicit FSMemoryKeyValueEventWriter(std::string output_file) {
        m_output_file = std::move(output_file);
        eventFile = m_output_file + LOG_EXTENSION;
        storage = new MemoryEventStorage();
        storage->initialize();
    }

    int write(Event *event) override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            storage->addEvent(kvEvent);
            return 0;
        }

        return -1;
    }

private:
//    std::list<Event *> *events;
    MemoryEventStorage *storage;
};


#endif //CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITER_H
