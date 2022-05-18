//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H


#include "EventWriterFactory.h"

#include <utility>
#include "KeyValueEventWriter.h"

class KeyValueEventWriterFactory : public EventWriterFactory {
public:
    explicit KeyValueEventWriterFactory(std::string output_file, int maxPayloadSize_) : m_output_file(
            std::move(output_file)), payloadSize(maxPayloadSize_) {}

    [[nodiscard]] EventWriter *getWriter() const override {
        return new KeyValueEventWriter(this->m_output_file, payloadSize);
    }

private:
    std::string m_output_file;
    int payloadSize;
};


#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITERFACTORY_H
