//
// Created by pablo on 21/05/2022.
//

#ifndef CHRONOSQL_POC_CONFIGURATIONMANAGER_H
#define CHRONOSQL_POC_CONFIGURATIONMANAGER_H


#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>
#include "../include/rapidjson/document.h"
#include "Enumerations.h"
#include "ConfigConstants.h"

using namespace rapidjson;
using namespace Enumerations;
using namespace ConfigConstants;

class ConfigurationManager {

public:

    // The source where the configuration will be read from
    std::string configurationFile;

    // Common configuration
    int nEvents;
    EventType eventType;
    std::string outputFile;

    // Key-value configuration
    int payloadSize = 0;
    int payloadVariation = 0;
    int fixedPayloadSize = 0;

    ConfigurationManager(std::string configurationFile_) : configurationFile(std::move(configurationFile_)) {
        Document configDoc = getJsonDocument(configurationFile);
        config(configDoc, NUMBER_EVENTS, nEvents);
        config(configDoc, EVENT_TYPE, eventType);
        config(configDoc, OUTPUT_FILE, outputFile);

        if (eventType == EventType::KEY_VALUE) {
            config(configDoc, PAYLOAD_SIZE, payloadSize);
            config(configDoc, PAYLOAD_VARIATION, payloadVariation);
            config(configDoc, FIXED_PAYLOAD_SIZE, fixedPayloadSize);
        }
    }


private:
    Document getJsonDocument(const std::string &filename) {
        // open the JSON file
        std::ifstream jFile(filename);
        if (!jFile.is_open()) {
            std::cout << "Unable to open configuration file " << filename << std::endl;
            exit(-1);
        }

        // read the file contents
        std::stringstream contents;
        contents << jFile.rdbuf();

        rapidjson::Document doc;
        doc.Parse(contents.str().c_str());  // parse the JSON from the string contents
        return doc;
    }

    void config(rapidjson::Document &doc, const char *member, int &variable) {
        assert(doc.HasMember(member));
        assert(doc[member].IsInt());
        variable = doc[member].GetInt();
    }

    void config(rapidjson::Document &doc, const char *member, std::string &variable) {
        assert(doc.HasMember(member));
        assert(doc[member].IsString());
        variable = doc[member].GetString();
    }

    void config(rapidjson::Document &doc, const char *member, EventType &variable) {
        assert(doc.HasMember(member));
        assert(doc[member].IsString());
        variable = EventTypeValues.find(doc[member].GetString())->second;
    }
};


#endif //CHRONOSQL_POC_CONFIGURATIONMANAGER_H
