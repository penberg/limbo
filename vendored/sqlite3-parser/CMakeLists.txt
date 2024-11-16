cmake_minimum_required(VERSION 3.6)
project(rlemon)

set(SOURCE_FILES third_party/lemon/lemon.c)
add_executable(rlemon ${SOURCE_FILES})