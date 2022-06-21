cmake_minimum_required(VERSION 3.22)
project(mpi_test)

set(CMAKE_CXX_STANDARD 14)

find_package(MPI REQUIRED)

add_executable(mpi_test main.cpp Executor.cpp Executor.h)

target_link_libraries(mpi_test PUBLIC MPI::MPI_CXX)
