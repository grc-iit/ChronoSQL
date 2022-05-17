cmake_minimum_required(VERSION 3.22)
project(chronoSQL_poc)

set(CMAKE_CXX_STANDARD 17)

add_executable(chronoSQL_poc main.cpp EventWriter.cpp EventWriter.h KeyValueEventWriter.cpp KeyValueEventWriter.h EventWriterFactory.cpp EventWriterFactory.h KeyValueEventWriterFactory.cpp KeyValueEventWriterFactory.h)
