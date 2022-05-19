cmake_minimum_required(VERSION 3.22)
project(chronoSQL_poc)

set(CMAKE_CXX_STANDARD 17)

add_executable(chronoSQL_poc main.cpp EventWriter/EventWriter.h EventWriter/KeyValueEventWriter.h EventWriter/EventWriterFactory.h EventWriter/KeyValueEventWriterFactory.h EventGenerator/EventGenerator.h EventGenerator/KeyValueEventGenerator.h Event/Event.h Event/KeyValueEvent.h EventGenerator/EventGeneratorFactory.h EventGenerator/KeyValueEventGeneratorFactory.h)
