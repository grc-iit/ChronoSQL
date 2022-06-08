cmake_minimum_required(VERSION 3.22)
project(chronoSQL_poc)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} "-static")

add_executable(chronoSQL_poc main.cpp event_writer/EventWriter.h event_writer/FSKeyValueEventWriter.h event_writer/FSMemoryKeyValueEventWriterFactory.h event_writer/IndexedKeyValueEventWriterFactory.h event_writer/EventWriterFactory.h event_writer/FSKeyValueEventWriterFactory.h event_generator/EventGenerator.h event_generator/KeyValueEventGenerator.h event/Event.h event/KeyValueEvent.h event_generator/KeyValueEventGeneratorFactory.h event_generator/EventGeneratorFactory.h config/ConfigurationManager.h config/Enumerations.h config/ConfigConstants.h config/ConfigurationValues.h event_writer/IndexedKeyValueEventWriter.h chronolog/ChronoLog.h common/typedefs.h event_reader/EventReader.h event_reader/FSEventReader.h event_reader/EventReaderFactory.h event_reader/FSEventReaderFactory.h common/Constants.h event_writer/FSMemoryKeyValueEventWriter.h event_reader/MemEventReader.h event_reader/MemEventReaderFactory.h common/MemoryEventStorage.h)
