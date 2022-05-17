//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H


#include "EventWriterFactory.h"
#include "KeyValueEventWriter.h"

class KeyValueEventWriterFactory : public EventWriterFactory {
public:
    EventWriter* getWriter() const override {
        return new KeyValueEventWriter();
    }
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
