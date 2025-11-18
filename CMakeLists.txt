cmake_minimum_required(VERSION 3.30)
set(CMAKE_C_STANDARD 17)

project(RabbitSort C)
set(CJSON_LIB /usr/local/lib)
find_library(cjson libcjson.so libcjson.dylib ${CJSON_LIB}/ REQUIRED)


#Genearate Tuples
add_executable(tupleGen TupleGen.c)

#Check Sorting Results
add_executable(check SmallCheck.c)
target_link_libraries(check PRIVATE cjson)
