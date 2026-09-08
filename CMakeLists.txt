# 3.21 is the real floor: PROJECT_IS_TOP_LEVEL below is 3.21+, and FetchContent_MakeAvailable
# is 3.14+. It also matches the floor slick-stream-buffer and slick-queue now state.
cmake_minimum_required(VERSION 3.21)

project(slick-stream-buffer-multiplexer
        VERSION 2.0.0
        DESCRIPTION "A C++ lock-free MPMC byte stream buffer multiplexer"
        LANGUAGES CXX)

# Options:
option(BUILD_SLICK_STREAM_BUFFER_MULTIPLEXER_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})

find_package(slick-stream-buffer 2.0.0 CONFIG QUIET)

if (NOT slick-stream-buffer_FOUND)
    include(FetchContent)

    # Disable slick-stream-buffer tests and examples, but allow installation
    set(BUILD_SLICK_STREAM_BUFFER_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        slick-stream-buffer
        GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer.git
        GIT_TAG v2.0.0 # See https://github.com/SlickQuant/slick-stream-buffer/releases for latest version
    )
    FetchContent_MakeAvailable(slick-stream-buffer)
else()
    message(STATUS "Found slick-stream-buffer ${slick-stream-buffer_VERSION}: ${slick-stream-buffer_DIR}")
endif()

find_package(slick-queue 2.0.0 CONFIG QUIET)

if (NOT slick-queue_FOUND)
    include(FetchContent)

    # Disable slick-queue tests and examples, but allow installation
    set(BUILD_SLICK_QUEUE_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        slick-queue
        GIT_REPOSITORY https://github.com/SlickQuant/slick-queue.git
        GIT_TAG v2.0.0 # See https://github.com/SlickQuant/slick-queue/releases for latest version
    )
    FetchContent_MakeAvailable(slick-queue)
else()
    message(STATUS "Found slick-queue ${slick-queue_VERSION}: ${slick-queue_DIR}")
endif()

add_library(slick-stream-buffer-multiplexer INTERFACE)
add_library(slick::stream_buffer_multiplexer ALIAS slick-stream-buffer-multiplexer)

target_include_directories(slick-stream-buffer-multiplexer INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(slick-stream-buffer-multiplexer PROPERTIES EXPORT_NAME stream_buffer_multiplexer)

# The headers require C++20 (concepts, [[no_unique_address]]). This must ride on the target, not
# on CMAKE_CXX_STANDARD: that variable applies only to targets built here, so installed consumers
# would compile the header under whatever standard they happen to use.
target_compile_features(slick-stream-buffer-multiplexer INTERFACE cxx_std_20)

target_link_libraries(slick-stream-buffer-multiplexer INTERFACE slick::stream_buffer slick::queue)
if(UNIX AND NOT APPLE)
    target_link_libraries(slick-stream-buffer-multiplexer INTERFACE rt atomic)
endif()

if(BUILD_SLICK_STREAM_BUFFER_MULTIPLEXER_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Installation rules
include(GNUInstallDirs)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Generate and install CMake config files for vcpkg compatibility
include(CMakePackageConfigHelpers)

# Create the config file
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/slick-stream-buffer-multiplexerConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-buffer-multiplexerConfig.cmake"
    INSTALL_DESTINATION lib/cmake/slick-stream-buffer-multiplexer
)

# Create the version file
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-buffer-multiplexerConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Install the config files
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-buffer-multiplexerConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-buffer-multiplexerConfigVersion.cmake"
    DESTINATION lib/cmake/slick-stream-buffer-multiplexer
)

# Install the target
install(TARGETS slick-stream-buffer-multiplexer
    EXPORT slick-stream-buffer-multiplexerTargets
)

# Install the export set
install(EXPORT slick-stream-buffer-multiplexerTargets
    FILE slick-stream-buffer-multiplexerTargets.cmake
    NAMESPACE slick::
    DESTINATION lib/cmake/slick-stream-buffer-multiplexer
)

message(STATUS "${PROJECT_NAME}: ${PROJECT_VERSION}")
