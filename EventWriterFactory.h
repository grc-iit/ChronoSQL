//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTWRITERFACTORY_H
#define CHRONOSQL_POC_EVENTWRITERFACTORY_H


#include "EventWriter.h"

class EventWriterFactory {
public:
    virtual EventWriter* getWriter() const = 0;
};


#endif //CHRONOSQL_POC_EVENTWRITERFACTORY_H
