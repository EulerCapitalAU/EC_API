cmake_minimum_required(VERSION 3.15)
project(EC_API_cpp_tests LANGUAGES CXX)


option(ENABLE_COVERAGE "Build with gcov instrumentation" OFF)

if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

execute_process(
    COMMAND python -m pybind11 --cmakedir
    OUTPUT_VARIABLE pybind11_CMAKE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
list(APPEND CMAKE_PREFIX_PATH ${pybind11_CMAKE_DIR})

find_package(GTest REQUIRED)      
find_package(pybind11 REQUIRED) 

enable_testing()
include(GoogleTest)

add_library(ec_api_common STATIC
    EC_API/common/data_extractors_cqg.cpp
    EC_API/common/tick_buffers.cpp
    EC_API/common/tick_buffers_ext.cpp
    # future production .cpp files land here, once, regardless of how many tests use them
)
target_include_directories(ec_api_common PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(ec_api_common PUBLIC pybind11::embed)

function(add_cpp_test name)
    add_executable(${name} tests/unit/buffers_cpp/${name}.cpp)
    target_link_libraries(${name} PRIVATE GTest::gtest_main ec_api_common)
    gtest_discover_tests(${name})
endfunction()

add_cpp_test(test_tick_stats)
add_cpp_test(test_data_extractors_cqg)
add_cpp_test(test_sliding_window_buffer)
add_cpp_test(test_ring_buffer)
