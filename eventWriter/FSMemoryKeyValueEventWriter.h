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

using namespace Constants;

class FSMemoryKeyValueEventWriter : public EventWriter {

public:
    explicit FSMemoryKeyValueEventWriter(std::string output_file) {
        m_output_file = std::move(output_file);
        eventFile = m_output_file + LOG_EXTENSION;
        events = new std::list<Event *>;
    }

    int write(Event *event) override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            events->push_back(event);
            return 0;
        }

        return -1;
    }

    std::list<Event *> *getEvents() {
        return events;
    }

private:
    std::list<Event *> *events;
};


#endif //CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITER_H
