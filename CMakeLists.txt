cmake_minimum_required(VERSION 3.30)
project(Newsort C)

set(CMAKE_C_STANDARD 11)

add_executable(Newsort
        RabbitSort.c)

include_directories(/opt/homebrew/Cellar/open-mpi/5.0.3_1/include)