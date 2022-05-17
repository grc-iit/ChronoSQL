cmake_minimum_required(VERSION 3.22)
project(chronoSQL_poc)

set(CMAKE_CXX_STANDARD 17)

add_executable(chronoSQL_poc main.cpp EventWriter/EventWriter.cpp EventWriter/EventWriter.h EventWriter/KeyValueEventWriter.cpp EventWriter/KeyValueEventWriter.h EventWriter/EventWriterFactory.cpp EventWriter/EventWriterFactory.h EventWriter/KeyValueEventWriterFactory.cpp EventWriter/KeyValueEventWriterFactory.h)
