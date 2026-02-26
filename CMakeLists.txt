cmake_minimum_required(VERSION 3.20)
project(apex-kv VERSION 2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Compiler flags ────────────────────────────────────────────────────────────
add_compile_options(
    -Wall -Wextra -Wpedantic
    $<$<CXX_COMPILER_ID:Clang>:-Wno-gnu-anonymous-struct>
    $<$<CONFIG:Release>:-O3>
    $<$<CONFIG:Release>:-march=native>
    $<$<CONFIG:Release>:-DNDEBUG>
    $<$<CONFIG:Debug>:-O0>
    $<$<CONFIG:Debug>:-g>
    $<$<CONFIG:Debug>:-fsanitize=address,undefined>
    $<$<CONFIG:RelWithDebInfo>:-O2>
    $<$<CONFIG:RelWithDebInfo>:-g>
    $<$<CONFIG:RelWithDebInfo>:-march=native>
)
add_link_options(
    $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)

# ── Static build support ──────────────────────────────────────────────────────
option(APEX_STATIC "Build fully static binary" OFF)
if(APEX_STATIC)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static -static-libgcc -static-libstdc++")
    set(BUILD_SHARED_LIBS OFF)
endif()

# ── Threads ───────────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)

# ── Main target ───────────────────────────────────────────────────────────────
add_executable(kv
    src/main.cpp
)

target_include_directories(kv PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kv PRIVATE
    Threads::Threads
)

# ── Install ───────────────────────────────────────────────────────────────────
install(TARGETS kv RUNTIME DESTINATION bin)

# ── Tests ─────────────────────────────────────────────────────────────────────
enable_testing()
add_test(NAME unit_tests
    COMMAND kv --test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# ── Summary ───────────────────────────────────────────────────────────────────
message(STATUS "")
message(STATUS "  APEX-KV  ${PROJECT_VERSION}")
message(STATUS "  Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "  Static     : ${APEX_STATIC}")
message(STATUS "  Install to : ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
message(STATUS "  Build:   cmake --build . --config Release")
message(STATUS "  Test:    ctest -V")
message(STATUS "  Install: cmake --install . --prefix /usr/local")
message(STATUS "")
