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

    static void generate(const char *indexFilename) {
        int pos = 0;

        std::ifstream file(indexFilename, std::ifstream::binary | std::ios::ate);
        std::streampos fileSize = file.tellg();

        while (pos <= fileSize) {
            char *id = new char[10];
            std::string offsetChar;

            file.seekg(pos);
            file.get(id, 11);
            auto eid = (std::time_t) strtol(id, nullptr, 10);
            file.seekg(pos + 10 + 1);
            pos += 11;

            std::getline(file, offsetChar, ';');
            pos += offsetChar.length() + 1;
            if (offsetChar.length() > 0 && pos <= fileSize) {
                addEntry(eid, std::stoi(offsetChar));
            }
        }
    }
};

std::map<EID, long> MemoryIndex::index;

#endif //CHRONOSQL_MEMORYINDEX_H
