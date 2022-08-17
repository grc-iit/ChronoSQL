# ChronoSQL

SQL plugin for the [Chronolog project](http://www.cs.iit.edu/~scs/assets/projects/ChronoLog/ChronoLog.html) developed with C++ 17.

## Configuration properties
To run ChronoSQL, a JSON file with configuration properties must be provided. The repository includes one already configured. The available properties are the following:

- NUMBER_EVENTS: Number of events to be generated
- EVENT_TYPE: The type of event storage to be used (FIXED_KEY_VALUE, naive disk; MEMORY_KEY_VALUE, in-memory; INDEXED_KEY_VALUE, indexed disk)
- PAYLOAD_SIZE: The size of the payload of the events
- INDEX_INTERVAL_BYTES: The byte threshold to create a new index entry
- PAYLOAD_VARIATION: The variation in the payload size (for the event generator)
- FIXED_PAYLOAD_SIZE: The desired payload size (for the event generator)
- LOWER_TIMESTAMP: The lowest timestamp to be generated (for the event generator)
- HIGHER_TIMESTAMP: The highest timestamp to be generated (for the event generator)
- HIDE_OUTPUT: Whether to hide the SQL output or not (useful for running tests)
- N_EXECUTIONS: Number of executions per query (testing)
- SQL_FILE_PATH: The file with the SQL queries (testing)

## Project dependencies

### RapidJSON
JSON parser library to read configuration files. It is header-only, so it is already present in the project. [GitHub](https://github.com/Tencent/rapidjson/).

### Hyrise SQL parser
SQL parser used by ChronoSQL. It has been modified to support wider syntax, so the ChronoSQL version of the project is available [here](https://github.com/pabloprz/sql-parser).

The commits in this project can be used as a reference for future additions to the library.

## Environment variables

The following environment variables should be set so that ChronoSQL knows the installation directory of the SQL parser:

CMAKE_LIBRARY_PATH=home/pablo/lib; CXXFLAGS=/home/pablo/lib; CXX_FLAGS=/home/pablo/lib

(Replace /home/pablo/lib by your library directory).

## Installation
1. Clone the project
2. Clone the SQL parser project
3. Compile the SQL parser as specified in their documentation
4. Set the environment variables of the ChronoSQL project to point to the location of the installed parser (see Clion environment variables)
5. Compile the ChronoSQL project

## Execution
After installation, the parser can be executed as

`./ChronoSQL ../config.json`

To generate events, use the -g flag with <name_of_file> <n_events> as follows:
`./ChronoSQL ../config.json -g mem_10k 10000 mem_100k 100000 mem_1m 1000000 mem_10m 10000000`
This would result in the generation of 4 logs.

Also, indexes can be loaded from a file using the -i flag:
`./ChronoSQL ../config.json -i indexed_10k indexed_100k indexed_1m indexed_10m`

Finally, passing the -t flag (at the end of the list of arguments) enters the testing mode.

## Documentation
Documentation about the design of ChronoSQL and advanced topics such as indexing can be found inside the `/docs` folder.
