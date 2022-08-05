//
// Created by pablo on 27/05/2022.
//

#ifndef ChronoSQL_INDEXEDKEYVALUEEVENTWRITERFACTORY_H
#define ChronoSQL_INDEXEDKEYVALUEEVENTWRITERFACTORY_H


#include <string>
#include "../config/ConfigurationValues.h"
#include "EventWriter.h"
#include "IndexedKeyValueEventWriter.h"

class IndexedKeyValueEventWriterFactory {

public:
    explicit IndexedKeyValueEventWriterFactory(const ConfigurationValues *configurationManager) :
            payloadSize(configurationManager->payloadSize),
            indexIntervalBytes(configurationManager->indexIntervalBytes) {}

    [[nodiscard]] EventWriter *getWriter() const {
        return new IndexedKeyValueEventWriter(this->payloadSize, this->indexIntervalBytes);
    }

private:
    int payloadSize;
    int indexIntervalBytes;
};


#endif //ChronoSQL_INDEXEDKEYVALUEEVENTWRITERFACTORY_H
