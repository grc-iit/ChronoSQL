//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H


#include "EventWriterFactory.h"

#include <utility>
#include "KeyValueEventWriter.h"
#include "../Config/ConfigurationManager.h"

class KeyValueEventWriterFactory {
public:
    explicit KeyValueEventWriterFactory(const ConfigurationManager *configurationManager) :
            m_output_file(configurationManager->outputFile), payloadSize(configurationManager->payloadSize) {}

    [[nodiscard]] EventWriter *getWriter() const {
        return new KeyValueEventWriter(this->m_output_file, payloadSize);
    }

private:
    std::string m_output_file;
    int payloadSize;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
