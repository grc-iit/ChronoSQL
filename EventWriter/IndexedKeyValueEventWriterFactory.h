//
// Created by pablo on 27/05/2022.
//

#ifndef CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITERFACTORY_H
#define CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITERFACTORY_H


#include <string>
#include "../Config/ConfigurationValues.h"
#include "EventWriter.h"
#include "IndexedKeyValueEventWriter.h"

class IndexedKeyValueEventWriterFactory {

public:
    explicit IndexedKeyValueEventWriterFactory(const ConfigurationValues *configurationManager) :
            m_output_file(configurationManager->outputFile) {}

    [[nodiscard]] EventWriter *getWriter() const {
        return new IndexedKeyValueEventWriter(this->m_output_file);
    }

private:
    std::string m_output_file;
};


#endif //CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITERFACTORY_H
