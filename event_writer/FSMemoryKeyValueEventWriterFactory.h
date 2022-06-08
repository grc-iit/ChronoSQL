//
// Created by pablo on 30/05/2022.
//

#ifndef CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITERFACTORY_H
#define CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITERFACTORY_H


#include "../config/ConfigurationValues.h"
#include "EventWriter.h"
#include "FSMemoryKeyValueEventWriter.h"

class FSMemoryKeyValueEventWriterFactory {

public:
    explicit FSMemoryKeyValueEventWriterFactory(const ConfigurationValues *configurationManager) :
            m_output_file(configurationManager->outputFile) {}

    [[nodiscard]] EventWriter *getWriter() const {
        return new FSMemoryKeyValueEventWriter(this->m_output_file);
    }

private:
    std::string m_output_file;
};


#endif //CHRONOSQL_POC_FSMEMORYKEYVALUEEVENTWRITERFACTORY_H
