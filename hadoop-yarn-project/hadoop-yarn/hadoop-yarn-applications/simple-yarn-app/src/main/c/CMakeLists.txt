cmake_minimum_required (VERSION 2.6)
project (pi)
add_executable (pi pi.c)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Ofast")
