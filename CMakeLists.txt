cmake_minimum_required(VERSION 3.6)
project(ml_between)

set(CMAKE_CXX_STANDARD 11)
set(SCIDB /opt/scidb/${SCIDB_VER})
set(SCIDB_THIRDPARTY /opt/scidb/${SCIDB_VER})
set(CMAKE_EXE_LINKER_FLAGS "-static -Wl,-soname,ml_between.so")

include_directories(/opt/scidb/15.12/include)
include_directories(${SCIDB_THIRDPARTY}/3rdparty/boost/include)
include_directories(${SCIDB}/include)

link_libraries(.)
link_libraries(${SCIDB}/lib ${SCIDB_THIRDPARTY}/3rdparty/boost/lib)

set(SOURCE_FILES LogicalBCBetween.cpp plugin.cpp PhysicalBCBetween.cpp BCBetweenArray.cpp BCBetweenArray.h)
add_library(ml_between SHARED ${SOURCE_FILES})