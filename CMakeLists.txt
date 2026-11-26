# SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(flux_job_pubsub_v2 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# GCC 15 may keep libatomic in its internal library directory.
# Cap'n Proto's CMake detection does not always search that directory.
execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libatomic.so
    OUTPUT_VARIABLE GCC_LIBATOMIC
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(EXISTS "${GCC_LIBATOMIC}")
    get_filename_component(GCC_LIBATOMIC_DIR
                           "${GCC_LIBATOMIC}"
                           DIRECTORY)

    list(PREPEND CMAKE_LIBRARY_PATH "${GCC_LIBATOMIC_DIR}")
    link_directories("${GCC_LIBATOMIC_DIR}")

    message(STATUS "Using GCC libatomic: ${GCC_LIBATOMIC}")
else()
    message(FATAL_ERROR
        "Could not locate libatomic using ${CMAKE_CXX_COMPILER}"
    )
endif()

set(SEANERBUS_CPP_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/seanerbus-cpp"
    CACHE PATH "Updated SeanerBUS C++ bindings")

find_package(PostgreSQL REQUIRED)
find_package(CapnProto CONFIG REQUIRED)

if(NOT EXISTS "${SEANERBUS_CPP_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "SeanerBUS C++ source missing at ${SEANERBUS_CPP_DIR}")
endif()

add_subdirectory("${SEANERBUS_CPP_DIR}" "${CMAKE_BINARY_DIR}/seanerbus-cpp")

add_library(hpc_job_database_v2 STATIC
    src/address.cpp
    src/database.cpp
)
target_include_directories(hpc_job_database_v2 PUBLIC include)
target_link_libraries(hpc_job_database_v2 PUBLIC PostgreSQL::PostgreSQL seanerbus)

add_executable(flux_lifecycle_daemon_v2
    src/flux_lifecycle_daemon.cpp
)
target_include_directories(flux_lifecycle_daemon_v2 PRIVATE include)
target_link_libraries(flux_lifecycle_daemon_v2 PRIVATE hpc_job_database_v2 seanerbus CapnProto::capnp CapnProto::kj atomic)

set_target_properties(flux_lifecycle_daemon_v2 PROPERTIES OUTPUT_NAME jobs-database-daemon)

include(GNUInstallDirs)
install(TARGETS flux_lifecycle_daemon_v2 RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES sql/001_schema.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/jobs-database-daemon)
