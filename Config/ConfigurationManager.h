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
#include "ConfigurationValues.h"

using namespace rapidjson;
using namespace Enumerations;
using namespace ConfigConstants;

class ConfigurationManager {

public:
    explicit ConfigurationManager(const std::string &configurationFile) {
        configuration = new ConfigurationValues();
        Document configDoc = getJsonDocument(configurationFile);
        config(configDoc, NUMBER_EVENTS, configuration->nEvents);
        config(configDoc, EVENT_TYPE, configuration->eventType);
        config(configDoc, OUTPUT_FILE, configuration->outputFile);

        if (configuration->eventType == EventType::FIXED_KEY_VALUE) {
            config(configDoc, PAYLOAD_SIZE, configuration->payloadSize);
            config(configDoc, PAYLOAD_VARIATION, configuration->payloadVariation);
            config(configDoc, FIXED_PAYLOAD_SIZE, configuration->fixedPayloadSize);
        }
    }

    ConfigurationValues *getConfiguration() {
        return configuration;
    }


private:
    // The source where the configuration will be read from
    ConfigurationValues *configuration;

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
