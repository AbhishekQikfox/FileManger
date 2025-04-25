cmake_minimum_required(VERSION 3.10)
project(MyBasicService)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add executable
add_executable(myservice myservice.cpp)

# Link against Windows service libraries
target_link_libraries(myservice PRIVATE advapi32)
