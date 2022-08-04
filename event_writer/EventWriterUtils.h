//
// Created by pablo on 4/8/22.
//

#ifndef CHRONOSQL_EVENTWRITERUTILS_H
#define CHRONOSQL_EVENTWRITERUTILS_H

#include <cstring>

class EventWriterUtils {

public:

    EventWriterUtils(int _fixedPayloadSize) : fixedPayloadSize(_fixedPayloadSize) {}

    const char *trimByteSequence(const char *payload) {
        int receivedSize = strlen(payload);

        if (receivedSize == fixedPayloadSize) {
            return payload;
        }

        char *trimmed = new char[fixedPayloadSize + 1]; // Null terminating character (+1)

        if (receivedSize < fixedPayloadSize) {
            // Copy the source sequence
            strncpy(trimmed, payload, receivedSize);
            // And pad with whitespaces at the end
            for (int i = receivedSize; i < fixedPayloadSize; i++) {
                trimmed[i] = ' ';
            }
        } else {
            // Copy only until we reach the max size
            strncpy(trimmed, payload, fixedPayloadSize);
        }
        // Terminate with the null char
        trimmed[fixedPayloadSize] = '\0';

        return trimmed;
    }

private:
    int fixedPayloadSize;
};


#endif //CHRONOSQL_EVENTWRITERUTILS_H
