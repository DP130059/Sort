cmake_minimum_required(VERSION 3.30)
set(CMAKE_C_COMPILER /usr/local/bin/mpicc)
set(CMAKE_C_STANDARD 17)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

project(RabbitSort C)


find_package(MPI REQUIRED)
if (MPI_FOUND)
    message("Found MPI")
else (MPI_FOUND)
    message("Not Found MPI")
endif (MPI_FOUND)


include_directories("/opt/homebrew/Cellar/open-mpi/5.0.7/include")
include_directories("/usr/local/include/")

add_executable(Rabbit Refresh.c)
add_executable(smallCheck SmallCheck.c)
