//
// Created by pablo on 30/05/2022.
//

#ifndef ChronoSQL_FSMEMORYKEYVALUEEVENTWRITER_H
#define ChronoSQL_FSMEMORYKEYVALUEEVENTWRITER_H


#include <list>
#include "../event/Event.h"
#include "../common/Constants.h"
#include "EventWriter.h"
#include "../event/KeyValueEvent.h"
#include "../common/MemoryEventStorage.h"

using namespace Constants;

class FSMemoryKeyValueEventWriter : public EventWriter {

public:
    explicit FSMemoryKeyValueEventWriter() {
        MemoryEventStorage::initialize();
    }

    int write(const CID &cid, Event *event) override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        if (kvEvent != nullptr) {
            MemoryEventStorage::addEvent(cid, kvEvent);
            return 0;
        }

        return -1;
    }
};


#endif //ChronoSQL_FSMEMORYKEYVALUEEVENTWRITER_H
