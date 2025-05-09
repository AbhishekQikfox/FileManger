cmake_minimum_required(VERSION 3.16)
project(FullUnitTests)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(GTest CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(SQLite3 REQUIRED)

# front-end utils
add_library(frontend_utils INTERFACE)
target_include_directories(frontend_utils INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(frontend_utils INTERFACE OpenSSL::Crypto)

# back-end utils
add_library(backend_utils INTERFACE)
target_include_directories(backend_utils INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(backend_utils INTERFACE SQLite::SQLite3 OpenSSL::Crypto)

# front-end tests
add_executable(frontend_tests test_frontend.cpp)
target_link_libraries(frontend_tests
  PRIVATE
    frontend_utils
    GTest::gtest_main
)

# back-end tests
add_executable(backend_tests test_backend.cpp)
target_link_libraries(backend_tests
  PRIVATE
    backend_utils
    GTest::gtest_main
)

# integration tests
add_executable(integration_tests integration_tests.cpp)
target_link_libraries(integration_tests
  PRIVATE
    frontend_utils
    GTest::gtest_main
    OpenSSL::Crypto
)

enable_testing()
add_test(NAME FE COMMAND frontend_tests)
add_test(NAME BE COMMAND backend_tests)
add_test(NAME INT COMMAND integration_tests)
