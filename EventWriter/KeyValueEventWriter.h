//
// Created by pablo on 16/05/2022.
//

#ifndef CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_KEYVALUEEVENTWRITER_H


#include <iostream>
#include <utility>
#include "EventWriter.h"
#include "../Event/KeyValueEvent.h"

class KeyValueEventWriter : public EventWriter {
public:
    explicit KeyValueEventWriter(std::string output_file) {
        m_output_file = std::move(output_file);
    }

    [[nodiscard]] int writeToFile(Event *event) const override {
        auto *kvEvent = dynamic_cast<KeyValueEvent *>(event);
        std::ofstream output_file = KeyValueEventWriter::openFile(m_output_file);
        output_file << kvEvent->getTimestamp() << ',' << kvEvent->getPayload() << ';';
        output_file.close();
        return 0;
    }
};

#endif //CHRONOSQL_POC_KEYVALUEEVENTWRITER_H
