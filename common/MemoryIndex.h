//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_MEMORYINDEX_H
#define CHRONOSQL_MEMORYINDEX_H

#include <map>
#include "../common/typedefs.h"

class MemoryIndex {

public:
    static std::map<EID, long> index;

    static void addEntry(EID eid, long longValue) {
        index[eid] = longValue;
    }

    static long getClosestValue(EID eid) {
        return index.lower_bound(eid)->second;
    }
};

std::map<EID, long> MemoryIndex::index;

#endif //CHRONOSQL_MEMORYINDEX_H
