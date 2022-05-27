//
// Created by pablo on 26/05/2022.
//

#ifndef CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H
#define CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H


#include "EventWriter.h"

class IndexedKeyValueEventWriter : public EventWriter {

private:
    const std::string index_file_extension = "index";
};


#endif //CHRONOSQL_POC_INDEXEDKEYVALUEEVENTWRITER_H
