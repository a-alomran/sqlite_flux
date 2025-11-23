# CMakeLists.txt
cmake_minimum_required(VERSION 3.15)
project(sqlite_flux VERSION 1.1.0 LANGUAGES CXX)

# =============================================================================
# Project Options
# =============================================================================

option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_TESTS "Build unit tests" OFF)

# =============================================================================
# C++ Standard
# =============================================================================

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# =============================================================================
# Output Directories
# =============================================================================

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# =============================================================================
# Dependencies
# =============================================================================

find_package(SQLite3 REQUIRED)

# =============================================================================
# Library Target
# =============================================================================

set(LIBRARY_SOURCES
    src/Analyzer.cpp
    src/QueryBuilder.cpp
    src/ValueVisitor.cpp
    src/ConnectionPool.cpp
    src/AsyncExecutor.cpp
    src/InsertBuilder.cpp
    src/UpdateBuilder.cpp
    src/DeleteBuilder.cpp
)

set(LIBRARY_HEADERS
    include/Analyzer.h
    include/ColumnValue.h
    include/QueryBuilder.h
    include/TableTypes.h
    include/ValueVisitor.h
    include/ConnectionPool.h
    include/AsyncExecutor.h
    include/InsertBuilder.h
    include/UpdateBuilder.h
    include/DeleteBuilder.h
)

add_library(sqlite_flux STATIC
    ${LIBRARY_SOURCES}
    ${LIBRARY_HEADERS}
)

# Alias for consistent naming (sqlite_flux::sqlite_flux)
add_library(sqlite_flux::sqlite_flux ALIAS sqlite_flux)

target_include_directories(sqlite_flux
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(sqlite_flux
    PUBLIC
        SQLite::SQLite3
)

set_target_properties(sqlite_flux PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

# Compiler warnings
if(MSVC)
    target_compile_options(sqlite_flux PRIVATE /W4 /WX-)
else()
    target_compile_options(sqlite_flux PRIVATE -Wall -Wextra -Wpedantic)
endif()

# =============================================================================
# Example Executables (Optional)
# =============================================================================

if(BUILD_EXAMPLES)
    # Example 1: Main demo (SELECT + DML operations)
    add_executable(sqlite_flux_demo
        examples/main.cpp
    )
    target_link_libraries(sqlite_flux_demo PRIVATE sqlite_flux::sqlite_flux)
    
    if(MSVC)
        target_compile_options(sqlite_flux_demo PRIVATE /W4 /WX-)
    else()
        target_compile_options(sqlite_flux_demo PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    # Example 2: Web application example (Connection Pool + Async + DML)
    add_executable(sqlite_flux_web
        examples/web_example.cpp
    )
    target_link_libraries(sqlite_flux_web PRIVATE sqlite_flux::sqlite_flux)
    
    if(MSVC)
        target_compile_options(sqlite_flux_web PRIVATE /W4 /WX-)
    else()
        target_compile_options(sqlite_flux_web PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endif()

# =============================================================================
# Tests (Optional)
# =============================================================================

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# =============================================================================
# Installation
# =============================================================================

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install library
install(TARGETS sqlite_flux
    EXPORT sqlite_flux-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install headers
install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/sqlite_flux
    FILES_MATCHING PATTERN "*.h"
)

# Install export targets
install(EXPORT sqlite_flux-targets
    FILE sqlite_flux-targets.cmake
    NAMESPACE sqlite_flux::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_flux
)

# Generate config file
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/sqlite_flux-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/sqlite_flux-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_flux
)

# Generate version file
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/sqlite_flux-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Install CMake config files
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/sqlite_flux-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/sqlite_flux-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_flux
)

# =============================================================================
# Configuration Summary
# =============================================================================

message(STATUS "")
message(STATUS "========== sqlite_flux Configuration ==========")
message(STATUS "Project:         ${PROJECT_NAME} ${PROJECT_VERSION}")
message(STATUS "Build type:      ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard:    ${CMAKE_CXX_STANDARD}")
message(STATUS "Install prefix:  ${CMAKE_INSTALL_PREFIX}")
message(STATUS "SQLite3 found:   ${SQLite3_FOUND}")
message(STATUS "Build examples:  ${BUILD_EXAMPLES}")
message(STATUS "Build tests:     ${BUILD_TESTS}")
message(STATUS "===============================================")
message(STATUS "")