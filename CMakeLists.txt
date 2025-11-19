cmake_minimum_required(VERSION 3.30)
set(CMAKE_C_STANDARD 17)
project(RabbitSort C)
set(CJSON_LIB /usr/local/lib)
find_library(cjson libcjson.so libcjson.dylib ${CJSON_LIB}/ REQUIRED)


#Genearate Tuples
add_executable(tupleGen TupleGen.c)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(my_app PRIVATE
            $<$<CONFIG:Release>:-Ofast>
    )
endif()

#Check Sorting Results
add_executable(check SmallCheck.c)
target_link_libraries(check PRIVATE cjson)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(my_app PRIVATE
            $<$<CONFIG:Release>:-Ofast>
    )
endif()


#Rabbit Sort
find_package(MPI REQUIRED)
add_executable(rabbitSort Refresh.c)
target_link_libraries(rabbitSort PRIVATE cjson)
target_link_libraries(rabbitSort PRIVATE MPI::MPI_C)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(my_app PRIVATE
            $<$<CONFIG:Release>:-Ofast>
    )
endif()