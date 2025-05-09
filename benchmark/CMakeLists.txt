cmake_minimum_required(VERSION 3.16)
project(ServiceBenchmarks)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find dependencies
find_package(benchmark CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)

# Add executable
add_executable(benchmark_service benchmark_service.cpp)

# Link libraries
target_link_libraries(benchmark_service PRIVATE 
    benchmark::benchmark 
    OpenSSL::SSL 
    OpenSSL::Crypto 
    nlohmann_json::nlohmann_json
    SQLite::SQLite3
)

# Include directories
target_include_directories(benchmark_service PRIVATE 
    "C:/Users/Training/Desktop/ProjectRoot/FrontendService"
    ${SQLite3_INCLUDE_DIRS}
)