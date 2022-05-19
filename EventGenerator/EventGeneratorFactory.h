//
// Created by pablo on 18/05/2022.
//

#ifndef CHRONOSQL_POC_EVENTGENERATORFACTORY_H
#define CHRONOSQL_POC_EVENTGENERATORFACTORY_H


#include "EventGenerator.h"

class EventGeneratorFactory {
    [[nodiscard]] virtual EventGenerator *getGenerator() const = 0;
};


#endif //CHRONOSQL_POC_EVENTGENERATORFACTORY_H
