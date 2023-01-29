cmake_minimum_required(VERSION 3.13)  # CMake version check
project(cseries)               # Create project "simple_example"
set(CMAKE_CXX_STANDARD 14)            # Enable c++14 standard

find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})

# Add main.cpp file of project root directory as source file
set(SOURCE_FILES series.cpp)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

# find_package(GTest REQUIRED)
# include_directories(${GTEST_INCLUDE_DIR})


# Add executable target with source files listed in SOURCE_FILES variable
add_executable(cseries ${SOURCE_FILES})
target_link_libraries(cseries ${PYTHON_LIBRARIES})


enable_testing()

add_executable(
  series_tests
  series_tests.cpp
#   series.cpp
#   series.h
)
target_link_libraries(
    series_tests
    GTest::gtest_main
    ${PYTHON_LIBRARIES}
)

include(GoogleTest)
gtest_discover_tests(series_tests)
