# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# PackSim -- a packet-level discrete-event simulator for AI training fabrics.
#
# This is the open-source build. Internally packsim is built with Buck, and the
# BUCK files are stripped from the export, so nothing here is generated from
# them: this file is the whole build system the exported repository has.
#
# The source layout it assumes is the EXPORT's layout, not fbcode's. ShipIt
# maps `fbcode/ai_simulation/` to the repository root and rewrites every
# `ai_simulation/packsim/...` include to `packsim/...`, so sources live under
# `packsim/` and include each other as `"packsim/..."`. That is why the include
# root below is the repository root rather than `packsim/`.
#
# packsim needs seven third-party libraries. The least painful way to get them
# is getdeps, which ships under build/fbcode_builder/ and builds folly and
# fbthrift at the revisions pinned in build/deps/github_hashes/. The steps CI
# runs are in packsim/README.md, under "Building".
#
# Against dependencies already installed, this is an ordinary CMake project:
#
#   cmake -B _build -DCMAKE_BUILD_TYPE=RelWithDebInfo
#   cmake --build _build -j 8
#   ctest --test-dir _build
#
# A job count, not a bare `-j`: each compile needs up to about 1.5 GB, and a
# bare `-j` under the Makefiles generator starts them all at once.
#
# `_build`, not `build`: `build/` is a SHIPPED directory now -- it holds
# fbcode_builder and the getdeps revision pins -- so configuring into it would
# mix generated output with tracked sources, and the reflexive `rm -rf build`
# would delete `getdeps.py` and the find-modules. Same reason folly builds into
# `_build`.
#
# Neither invocation needs -DCMAKE_MODULE_PATH: `build/fbcode_builder/` is part
# of the export, and the entry added below picks the find-modules up from there.
# See the note beside it.
#
# `-DBUILD_TESTS=OFF` skips the test executables, which are most of the build.
# No count here: they are globbed out of every `oss_tests/` directory, so the
# number moves with every diff that adds a test, and a stale one in this header
# is not something the configure can catch. The configure prints the real figure
# as `packsim: N test executables`. With GCC or Clang on Linux, outside a Debug
# build they are built without debug info unless `-DPACKSIM_TEST_DEBUG_INFO=ON`.

cmake_minimum_required(VERSION 3.20)

project(packsim LANGUAGES CXX)

# Refused rather than worked around. An in-source configure scatters the cache,
# CMake's own scratch directories, the generated thrift tree and every binary
# through the checked out repository, alongside the `build/` directory that
# holds `getdeps.py` and the dependency pins -- so the obvious cleanup is the
# one that destroys them.
if(CMAKE_CURRENT_BINARY_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  message(FATAL_ERROR
    "in-source builds are not supported: the generated sources and binaries "
    "would land in the checked out repository. Build out of tree:\n"
    "  cmake -S ${CMAKE_CURRENT_SOURCE_DIR} -B <build-dir>")
endif()

option(BUILD_TESTS "Build packsim's unit tests" ON)
option(BUILD_DEMOS "Build the coroutine demo binary" ON)
option(PACKSIM_TEST_DEBUG_INFO "Keep debug info in non-Debug unit-test executables" OFF)
option(PACKSIM_HEAVY_TESTS "Also run PACKSIM_HEAVY_TEST_SOURCES (always built)" OFF)

# packsim uses C++20 coroutines (folly::coro), std::source_location and
# std::barrier. Nothing in the export needs C++23 -- the one `std::expected`
# mention is inside a comment.
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# GCC does not enable coroutine support by default below C++23.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fcoroutines>)
endif()

# The oldest compilers that build packsim with its dependencies: folly needs
# GCC 11, and packsim's C++20 needs Clang 16 (parenthesized aggregate
# initialization). Older ones fail with hundreds of errors instead of this.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
   AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
  message(FATAL_ERROR
    "packsim needs GCC 11 or newer; this is GCC ${CMAKE_CXX_COMPILER_VERSION}")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
       AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 16)
  message(FATAL_ERROR
    "packsim needs Clang 16 or newer; this is Clang ${CMAKE_CXX_COMPILER_VERSION}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# `find_package(Gflags)` and `find_package(GMock MODULE)` below resolve ONLY
# through fbcode_builder's FindGflags.cmake / FindGMock.cmake, so the configure
# fails unless the module path reaches a copy of them. The entry below is what
# supplies one, and it needs no flag from the caller: the export config maps
# `fbcode/opensource/fbcode_builder/` to `build/fbcode_builder/`, so a fresh
# clone carries the find-modules at exactly this path.
#
# That mapping is also what makes the getdeps invocation in the header runnable
# -- `getdeps.py` lives in the same shipped directory. One config entry, both
# problems; they were a single missing directory, not two gaps.
#
# getdeps does not close this from its side: its CMake builder appends nothing
# to CMAKE_MODULE_PATH. So if the mapping is ever dropped from
# opensource/shipit_config/facebook/ai_simulation, this line silently stops
# resolving -- a module path component that does not exist is skipped rather
# than diagnosed -- and the configure fails later at `find_package(Gflags)`
# instead of here.
list(APPEND CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/build/fbcode_builder/CMake"
)

# ---------------------------------------------------------------------------
# Dependencies
#
# Seven third-party libraries, all under permissive licences (Apache-2.0,
# BSL-1.0, BSD-3-Clause and MIT). None of them is part of this repository, and
# each keeps its own licence. The list is exhaustive:
# every angled include under packsim/ resolves to one of these, to the C++
# standard library, or to <fcntl.h>/<unistd.h>/<sys/resource.h>.
# ---------------------------------------------------------------------------

# CMake 3.30 removed FindBoost; Boost 1.70+ ships its own config package.
# Opting into the new policy where it exists keeps the config path taken on
# both sides of that version boundary instead of emitting a developer warning.
# Set before `find_package(folly)`, whose config package finds Boost itself.
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

find_package(fmt CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(Gflags REQUIRED)

# Boost is header-only here: <boost/circular_buffer.hpp> in packsim/queues and
# <boost/interprocess/mapped_region.hpp> in packsim/switch. No compiled
# component is linked, so no COMPONENTS argument.
find_package(Boost REQUIRED)
# Both routes into `find_package(Boost)` define `Boost::headers`: FindBoost has
# since CMake 3.15, which `cmake_minimum_required(VERSION 3.20)` above already
# guarantees, and Boost's own config package has since 1.70, which is the floor
# for having a config package at all. So this is an assertion, not a choice --
# there is no older alias left to fall back to, and silently linking nothing
# would surface as a confusing missing-header error in the first source that
# includes <boost/...> rather than here.
if(NOT TARGET Boost::headers)
  message(FATAL_ERROR
    "find_package(Boost) succeeded but defined no Boost::headers target. "
    "Expected it from FindBoost (CMake >= 3.15) or from Boost's own config "
    "package (Boost >= 1.70); this build supports no older spelling.")
endif()
set(PACKSIM_BOOST_TARGET Boost::headers)

find_package(magic_enum CONFIG REQUIRED)

# packsim uses fbthrift for codegen and (de)serialization only -- it declares
# no services, so the RPC stack (fizz/wangle/mvfst) is not needed and the
# getdeps manifest asks for `fbthrift = !rpc, !benchmark`. Linking
# thriftprotocol/thriftmetadata/thrifttype rather than thriftcpp2 keeps that
# true at the link step as well.
find_package(FBThrift REQUIRED)
# FBThriftConfig.cmake reports a missing compiler as a NOTFOUND value rather than
# failing, and ThriftLibrary.cmake then falls back to a bare `thrift` on PATH, so
# the build would get as far as the first codegen step before stopping.
if(NOT FBTHRIFT_COMPILER)
  message(FATAL_ERROR
    "fbthrift was found without its compiler (FBTHRIFT_COMPILER="
    "${FBTHRIFT_COMPILER}); install fbthrift with the thrift1 compiler.")
endif()

# The schemas declare an empty `package;` on purpose -- PackSim's types carry no
# Thrift URI -- and thrift1 warns about that form once per schema unless its
# `empty_or_no_package` validation is off. A schema with no `package` directive
# at all still warns, under `no_package`.
#
# `thrift_generate` takes no compiler flags and puts THRIFT_COMPILER in each
# codegen step's DEPENDS as well as its COMMAND, so the flag cannot ride along
# in the variable; it goes in through a wrapper. The wrapper stands in for the
# compiler's timestamp as well: a rebuilt compiler reconfigures the build, and
# the touch below then makes every codegen step out of date. `file(CONFIGURE)`
# rather than `file(WRITE)` because it leaves an unchanged wrapper untouched,
# so a reconfigure alone does not regenerate every schema.
set(THRIFT_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/thrift1")
file(CONFIGURE OUTPUT "${THRIFT_COMPILER}" CONTENT [=[#!/bin/sh
exec "@FBTHRIFT_COMPILER@" --extra-validation empty_or_no_package=none "$@"
]=] @ONLY)
file(CHMOD "${THRIFT_COMPILER}" PERMISSIONS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE)
set_property(DIRECTORY APPEND PROPERTY
  CMAKE_CONFIGURE_DEPENDS "${FBTHRIFT_COMPILER}")
if("${FBTHRIFT_COMPILER}" IS_NEWER_THAN "${THRIFT_COMPILER}")
  file(TOUCH "${THRIFT_COMPILER}")
endif()
# ThriftLibrary.cmake ships alongside the fbthrift headers.
list(APPEND CMAKE_MODULE_PATH "${FBTHRIFT_INCLUDE_DIR}/thrift")
include(ThriftLibrary)

# ---------------------------------------------------------------------------
# Thrift codegen
#
# One object library per .thrift file, all folded into packsim_thrift. The
# generated headers land at ${PACKSIM_GENERATED_ROOT}/packsim/if/gen-cpp2/,
# which is what the sources' `#include "packsim/if/gen-cpp2/..._types.h"`
# expects once that root is on the include path.
#
# The `json` option is required: without it the generated types lose the JSON
# (de)serialisation the simulator's config and result files depend on.
# ---------------------------------------------------------------------------

# Generated sources go under their own root rather than straight into the build
# directory, and the extra path component is load-bearing. The generated tree
# has to be `<root>/packsim/if/gen-cpp2/` for the sources'
# `#include "packsim/if/gen-cpp2/..."` to resolve, which means creating a
# directory called `packsim` inside whatever root is chosen. With the build
# directory as that root, `<build>/packsim` is a directory -- and
# `add_executable(packsim ...)` below writes its binary to exactly that path,
# so the link fails with "cannot open output file packsim: Is a directory"
# after every source in the project has compiled. Keeping the generated tree
# one level down leaves the executable names free.
set(PACKSIM_GENERATED_ROOT "${CMAKE_CURRENT_BINARY_DIR}/gen")

set(PACKSIM_THRIFT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/packsim/if")
set(PACKSIM_THRIFT_OUT "${PACKSIM_GENERATED_ROOT}/packsim/if")

# thrift1 refuses to create its own output directory ("Output path ... is
# unusable or not a directory") and ThriftLibrary.cmake does not create it
# either, so every generator step fails on a clean build tree without this.
file(MAKE_DIRECTORY "${PACKSIM_THRIFT_OUT}")

# Kept in sync with packsim/if/*.thrift. Listed rather than globbed so that a
# new IDL file is a deliberate edit here -- see the source-inventory note at
# the bottom of this file.
#
# The drift check below globs one directory level, so this list and that check
# both cover exactly the `.thrift` files sitting directly in `packsim/if/`; a
# schema in a subdirectory is outside both by construction.
set(PACKSIM_THRIFT_FILES
  config
  nic_config
  qp_pair_config
  results
  roce_v2_config
  switch_config
  switches
  topology
  transport_config
  transport_stats
  workload
)

# Checked BEFORE the codegen loop below, because the loop consumes the same list
# and one of the two drift directions reaches it first. A .thrift ADDED to the
# tree is inert until something asks for it, so either order catches that. One
# REMOVED or RENAMED is a path in this list that no longer exists, and handing
# that to `thrift_generate` fails inside the generator step with an error naming
# neither list -- which is precisely the diagnostic this block exists to replace.
file(GLOB packsim_thrift_on_disk CONFIGURE_DEPENDS
  "${PACKSIM_THRIFT_DIR}/*.thrift")
set(packsim_thrift_expected "")
foreach(thrift_base IN LISTS PACKSIM_THRIFT_FILES)
  list(APPEND packsim_thrift_expected "${PACKSIM_THRIFT_DIR}/${thrift_base}.thrift")
endforeach()
list(SORT packsim_thrift_on_disk)
list(SORT packsim_thrift_expected)
if(NOT packsim_thrift_on_disk STREQUAL packsim_thrift_expected)
  message(FATAL_ERROR
    "PACKSIM_THRIFT_FILES is out of sync with packsim/if/*.thrift.\n"
    "  on disk: ${packsim_thrift_on_disk}\n"
    "  listed : ${packsim_thrift_expected}")
endif()

# fbthrift from before October 15, 2026 has only the positional
# `thrift_generate`, wrapped by `thrift_object`, and the fbthrift revision the
# export pins is one of those. Drop the `thrift_object` branch once the pin
# moves past it.
foreach(thrift_base IN LISTS PACKSIM_THRIFT_FILES)
  if(COMMAND thrift_object)
    thrift_object(
      "${thrift_base}"    # file_name
      ""                  # services -- packsim declares none
      "cpp2"              # language
      "json"              # options
      "${PACKSIM_THRIFT_DIR}"
      "${PACKSIM_THRIFT_OUT}"
      "packsim/if"        # include_prefix
      THRIFT_INCLUDE_DIRECTORIES
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${FBTHRIFT_INCLUDE_DIR}"
    )
  else()
    thrift_generate(
      "${PACKSIM_THRIFT_DIR}/${thrift_base}.thrift"
      LANGUAGE cpp2
      OPTIONS json
      OUT_SOURCES "${thrift_base}-cpp2-SOURCES"
      INCLUDE_DIRECTORIES
        # Resolves `include "packsim/if/<other>.thrift"`.
        "${CMAKE_CURRENT_SOURCE_DIR}"
        # Resolves `include "thrift/annotation/{thrift,cpp}.thrift"`, which
        # come from fbthrift itself and are deliberately not rewritten on
        # export.
        "${FBTHRIFT_INCLUDE_DIR}"
      OUTPUT_DIRECTORY "${PACKSIM_THRIFT_OUT}"
      INCLUDE_PREFIX "packsim/if"
    )
    add_library("${thrift_base}-cpp2-obj" OBJECT ${${thrift_base}-cpp2-SOURCES})
  endif()
endforeach()

# The thrift compiler only reads .thrift sources, so the codegen steps are
# independent of each other -- but compiling one file's generated .cpp needs
# the generated headers of every file it includes. Gating all the object
# libraries on one target that generates every file is the simple way to state
# that; the codegen itself is fast enough that the lost parallelism does not
# matter. Depending on the generated sources runs the generator, which writes
# the headers too.
set(packsim_thrift_generated "")
foreach(thrift_base IN LISTS PACKSIM_THRIFT_FILES)
  list(APPEND packsim_thrift_generated ${${thrift_base}-cpp2-SOURCES})
endforeach()
add_custom_target(packsim_thrift_codegen DEPENDS ${packsim_thrift_generated})

set(packsim_thrift_objects "")
foreach(thrift_base IN LISTS PACKSIM_THRIFT_FILES)
  add_dependencies("${thrift_base}-cpp2-obj" packsim_thrift_codegen)
  target_include_directories("${thrift_base}-cpp2-obj" PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${PACKSIM_GENERATED_ROOT}"
    "${FBTHRIFT_INCLUDE_DIR}"
  )
  target_link_libraries("${thrift_base}-cpp2-obj" PRIVATE
    Folly::folly
    fmt::fmt
  )
  # Nothing in generated code is ours to fix, and its setters and comparisons
  # read every field, so a field marked `@thrift.Deprecated` warns in its own
  # generated sources. Hand-written code that reads one still warns.
  target_compile_options("${thrift_base}-cpp2-obj" PRIVATE
    -Wno-deprecated-declarations
  )
  list(APPEND packsim_thrift_objects "$<TARGET_OBJECTS:${thrift_base}-cpp2-obj>")
endforeach()

add_library(packsim_thrift STATIC ${packsim_thrift_objects})
# Every source here is a `$<TARGET_OBJECTS:...>` generator expression, and those
# carry no file extension for CMake to infer a language from -- with nothing else
# in the target it gives up at generate time with "can not determine linker
# language". The objects are all C++, so say so.
set_target_properties(packsim_thrift PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(packsim_thrift PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${PACKSIM_GENERATED_ROOT}"
)
target_link_libraries(packsim_thrift PUBLIC
  FBThrift::thriftprotocol
  FBThrift::thriftmetadata
  FBThrift::thrifttype
  Folly::folly
  fmt::fmt
)

# ---------------------------------------------------------------------------
# The simulator library
#
# Globbed rather than enumerated, deliberately. What ships is decided by the
# ShipIt config, not by this file, and every .cpp that survives the export and
# is not a unit test belongs in this library -- so a list here would be a
# second, hand-maintained copy of a decision made elsewhere, and the only thing
# it could ever do is disagree. CONFIGURE_DEPENDS re-runs the glob on rebuild.
# ---------------------------------------------------------------------------

file(GLOB_RECURSE packsim_all_sources CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/*.cpp")
file(GLOB_RECURSE packsim_headers CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/*.h")

# Split on each source's path RELATIVE to this file, never on the absolute glob
# result. The materialised export can be written anywhere, and a destination
# that happens to contain an `oss_tests` component -- `cmake -S
# /tmp/oss_tests/packsim-export` is not a strange thing to type -- makes every
# absolute path match `/oss_tests/`, leaving `packsim_core` empty and building
# every source as a test executable instead. A relative path always begins
# `packsim/`, because that is what the glob above asked for, so `oss_tests` can
# never be its first component and nothing above the source root can reach this
# decision.
set(packsim_sources "")
set(packsim_test_sources "")
foreach(packsim_source IN LISTS packsim_all_sources)
  file(RELATIVE_PATH packsim_source_rel
    "${CMAKE_CURRENT_SOURCE_DIR}" "${packsim_source}")
  if(packsim_source_rel MATCHES "/oss_tests/")
    list(APPEND packsim_test_sources "${packsim_source}")
  else()
    list(APPEND packsim_sources "${packsim_source}")
  endif()
endforeach()

# The headers get the same split, on the same relative-path rule. Listing them
# on a target does not affect compilation either way -- it is what makes them
# show up under the target in an IDE -- but leaving the test headers on
# `packsim_core` would file them under the library even with `-DBUILD_TESTS=OFF`,
# which contradicts the split immediately above.
set(packsim_library_headers "")
foreach(packsim_header IN LISTS packsim_headers)
  file(RELATIVE_PATH packsim_header_rel
    "${CMAKE_CURRENT_SOURCE_DIR}" "${packsim_header}")
  if(NOT packsim_header_rel MATCHES "/oss_tests/")
    list(APPEND packsim_library_headers "${packsim_header}")
  endif()
endforeach()

# The three EXPORTED files with a main() of their own are executables, not
# library members. Every other `main()` in the tree belongs to a directory this
# repository does not contain, so the glob above never sees one.
#
# Both ways this list can drift are SILENT, so both are guarded below. It is
# tempting to think a new `main()` announces itself with a duplicate-symbol
# error, and that is wrong: `packsim_core` is a static archive, and a linker
# extracts only the members that resolve an undefined symbol. Nothing in
# `packsim.cpp` or `packsim-replay.cpp` references the stray object, so it is
# never pulled in, nothing collides, and the source ships compiled into a
# library with no executable of its own and no way for anyone to run it.
set(PACKSIM_MAIN_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/packsim.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/packsim-replay.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/kernel/coro/demo/coro_demo.cpp"
)

# `list(REMOVE_ITEM)` on a path that is no longer there does nothing and says
# nothing, so a rename would quietly leave the renamed `main()` in the library,
# where -- being a static archive member nothing references -- it would sit
# unlinked rather than announcing itself. Fail here instead, naming the path.
# Same for the test harnesses below.
function(packsim_require_present listing_name)
  foreach(required IN LISTS ARGN)
    if(NOT EXISTS "${required}")
      message(FATAL_ERROR
        "${listing_name} names a file that does not exist:\n"
        "  ${required}\n"
        "It was renamed or removed. Update the list -- it is enumerated because "
        "these files are exceptions to the globs above, so nothing else can "
        "notice.")
    endif()
  endforeach()
endfunction()

packsim_require_present(PACKSIM_MAIN_SOURCES ${PACKSIM_MAIN_SOURCES})
list(REMOVE_ITEM packsim_sources ${PACKSIM_MAIN_SOURCES})

# The other half of that drift, and the one the archive will not report. Read at
# configure time so a newly shipping `main()` fails here, naming itself, rather
# than being compiled into `packsim_core` and quietly never linked.
#
# A text scan, and therefore a HEURISTIC -- CMake cannot parse C++ at configure
# time, so this is a tripwire for the conventional spelling, not a proof. It
# misses an entry point written some other way (a return type on its own line, a
# macro-expanded `main`), and it would fire on any line that merely looks like
# one, such as `int main(` inside a raw string literal. Both are unlikely in a
# shipping library source and both fail in a legible direction: a miss leaves
# today's silent behaviour, and a false positive stops the configure with the
# path in hand. Reformat the offending line if that ever happens.
foreach(packsim_source IN LISTS packsim_sources)
  file(STRINGS "${packsim_source}" packsim_stray_main
    REGEX "^[ \t]*(int|auto)[ \t]+main[ \t]*\\(" LIMIT_COUNT 1)
  if(packsim_stray_main)
    message(FATAL_ERROR
      "a shipping source defines main() but is not in PACKSIM_MAIN_SOURCES:\n"
      "  ${packsim_source}\n"
      "As written it would compile into the packsim_core archive and never be "
      "linked into anything, so it would ship with no way to run it. Add it to "
      "PACKSIM_MAIN_SOURCES to give it an executable, or strip it from the "
      "export.")
  endif()
endforeach()

list(LENGTH packsim_sources packsim_source_count)
message(STATUS "packsim: ${packsim_source_count} library sources")

add_library(packsim_core STATIC ${packsim_sources} ${packsim_library_headers})

# No `INSTALL_INTERFACE` here on purpose. `install()` below takes the two
# runtime binaries and nothing else -- no library, no headers, no export set --
# so an installed-header include path would describe a layout this build never
# produces. Giving the library an install rule and an export set is a decision
# about shipping packsim as a consumable CMake package, which is worth making
# deliberately rather than implying it with an unreachable generator expression.
target_include_directories(packsim_core PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
  "$<BUILD_INTERFACE:${PACKSIM_GENERATED_ROOT}>"
)
# Guarded for the same reason as the gmock variables below: FindGflags derives
# this from the gflags config package, where it can come back empty or point at
# a path that is not there -- the module has a branch for exactly that on
# RedHat. The imported library carries its own usage requirements either way.
#
# Only the INCLUDE_DIR and the gmock defines are guarded, never the `_LIBRARY` /
# `_LIBRARIES` values fed to target_link_libraries below, and the asymmetry is
# the point rather than an omission: a NOTFOUND include directory is a hard
# generate-time failure, which is what the guard buys, while an empty link entry
# simply expands to nothing. There is also no NOTFOUND to catch on that side --
# in module mode `FIND_PACKAGE_HANDLE_STANDARD_ARGS` checks LIBGFLAGS_LIBRARY,
# so REQUIRED has already aborted the configure; in config mode the value comes
# straight from `gflags_LIBRARIES`. A guard there would buy nothing.
if(LIBGFLAGS_INCLUDE_DIR AND EXISTS "${LIBGFLAGS_INCLUDE_DIR}")
  target_include_directories(packsim_core SYSTEM PUBLIC
    ${LIBGFLAGS_INCLUDE_DIR}
  )
endif()
target_link_libraries(packsim_core PUBLIC
  packsim_thrift
  Folly::folly
  fmt::fmt
  ${PACKSIM_BOOST_TARGET}
  magic_enum::magic_enum
  ${LIBGFLAGS_LIBRARY}
)

# folly's XLOG prints __FILE__ verbatim. Stripping the source root keeps log
# lines reading `packsim/...`, the same as they do internally.
target_compile_definitions(packsim_core PUBLIC
  FOLLY_XLOG_STRIP_PREFIXES="${CMAKE_CURRENT_SOURCE_DIR}"
)

# ---------------------------------------------------------------------------
# Binaries
# ---------------------------------------------------------------------------

add_executable(packsim "${CMAKE_CURRENT_SOURCE_DIR}/packsim/packsim.cpp")
target_link_libraries(packsim PRIVATE packsim_core)

add_executable(packsim-replay
  "${CMAKE_CURRENT_SOURCE_DIR}/packsim/packsim-replay.cpp")
target_link_libraries(packsim-replay PRIVATE packsim_core)

# `install()` drops the build-tree RPATH, and getdeps installs every dependency
# under its own prefix, off the loader's default path -- so an installed binary
# fails at startup on the first shared library, `libglog.so`. This keeps the
# directories of the libraries it linked against; CMake leaves out the system
# ones, so a build against OS packages installs exactly as it did before.
set_target_properties(packsim packsim-replay PROPERTIES
  INSTALL_RPATH_USE_LINK_PATH ON
)

install(TARGETS packsim packsim-replay RUNTIME DESTINATION bin)

# With -DBUILD_DEMOS=OFF this source is compiled by nothing at all, because
# PACKSIM_MAIN_SOURCES removes it from packsim_core unconditionally. That is the
# intended reading of the option and not an oversight: it has a main(), so the
# alternative -- removing it only when demos are on -- would leave an entry
# point sitting in the static archive with nothing referencing it, which is
# exactly the silent failure PACKSIM_MAIN_SOURCES exists to prevent. Not
# building a source you asked not to build is the better of the two.
if(BUILD_DEMOS)
  add_executable(coro_demo
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/kernel/coro/demo/coro_demo.cpp")
  target_link_libraries(coro_demo PRIVATE packsim_core)
endif()

# An executable is written to ${CMAKE_CURRENT_BINARY_DIR}/<name>, so a directory
# of the same name anywhere in the build tree costs the link -- and costs it
# AFTER every source has compiled, with "cannot open output file <name>: Is a
# directory" and no mention of whatever created the directory. That is what
# putting the generated thrift tree at `<build>/packsim/if` used to do to the
# `packsim` binary. `PACKSIM_GENERATED_ROOT` keeps them apart; this makes the
# next collision a named configure failure instead.
#
# Deliberately not extended to the per-test executables below: their names are
# already constrained by `packsim_reserved_target_names`, and they are created
# in a loop over sources that the same loop would have to re-scan.
#
# `coro_demo` is only in the list when it is actually built -- with
# `-DBUILD_DEMOS=OFF` no such executable is linked, so a directory of that name
# could not break anything and aborting the configure over it would be a false
# positive.
set(PACKSIM_LINKED_BINARIES packsim packsim-replay)
if(BUILD_DEMOS)
  list(APPEND PACKSIM_LINKED_BINARIES coro_demo)
endif()
foreach(packsim_binary IN LISTS PACKSIM_LINKED_BINARIES)
  if(IS_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${packsim_binary}")
    message(FATAL_ERROR
      "the '${packsim_binary}' executable cannot be linked: the build tree "
      "already uses that path as a directory.\n"
      "  ${CMAKE_CURRENT_BINARY_DIR}/${packsim_binary}\n"
      "Something above created it -- generated sources belong under "
      "PACKSIM_GENERATED_ROOT (${PACKSIM_GENERATED_ROOT}), not directly in the "
      "build directory.")
  endif()
endforeach()

# ---------------------------------------------------------------------------
# Tests
#
# One executable per test source.
# Three sources under oss_tests/ are shared harnesses with no test cases of
# their own; they go into a support library that every test links.
# ---------------------------------------------------------------------------

if(BUILD_TESTS)
  find_package(GMock MODULE REQUIRED)
  enable_testing()

  set(PACKSIM_TEST_SUPPORT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/transport/oss_tests/roce-v2-test-config.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/oss_tests/rank-harness.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/parallel_qp/oss_tests/channel-manager-harness.cpp"
  )
  packsim_require_present(
    PACKSIM_TEST_SUPPORT_SOURCES ${PACKSIM_TEST_SUPPORT_SOURCES})
  list(REMOVE_ITEM packsim_test_sources ${PACKSIM_TEST_SUPPORT_SOURCES})

  add_library(packsim_test_support STATIC ${PACKSIM_TEST_SUPPORT_SOURCES})
  target_link_libraries(packsim_test_support PUBLIC
    packsim_core
    ${LIBGMOCK_LIBRARIES}
  )
  # FindGMock reads these off GTest::gtest when gtest was found in config mode,
  # where they can legitimately come back empty or NOTFOUND -- the imported
  # target already carries its own usage requirements. Only forward them when
  # they are real, or CMake fails at generate time on a NOTFOUND include dir.
  if(LIBGMOCK_INCLUDE_DIR AND EXISTS "${LIBGMOCK_INCLUDE_DIR}")
    target_include_directories(packsim_test_support SYSTEM PUBLIC
      ${LIBGMOCK_INCLUDE_DIR}
    )
  endif()
  if(LIBGMOCK_DEFINES)
    target_compile_definitions(packsim_test_support PUBLIC ${LIBGMOCK_DEFINES})
  endif()
  # Every test executable links its own copy of packsim_core, and at
  # RelWithDebInfo its own copy of that library's debug info: about 14.6 GB
  # across the test executables, against about 1 GB without it, which is more
  # disk than a GitHub-hosted runner has. The library and every other binary
  # keep theirs, and so does a Debug build, which is configured to be debugged.
  # `--strip-debug` keeps the symbol table, so a backtrace still names
  # functions. `-g0` is a GCC/Clang spelling, and `--strip-debug` is understood
  # by GNU ld, gold, lld and mold, the Linux linkers those drive; any other
  # toolchain keeps its debug info.
  if(NOT PACKSIM_TEST_DEBUG_INFO
     AND CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$"
     AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Target options come after CMAKE_CXX_FLAGS_<CONFIG>, so -g0 overrides -g.
    target_compile_options(packsim_test_support PUBLIC
      $<$<NOT:$<CONFIG:Debug>>:-g0>)
    target_link_options(packsim_test_support INTERFACE
      $<$<NOT:$<CONFIG:Debug>>:LINKER:--strip-debug>)
  endif()

  # Built with the rest, but registered disabled unless -DPACKSIM_HEAVY_TESTS=ON.
  # Each runs on about one core; peak RSS and wall clock on a recent x86-64
  # server: all-reduce 1.7 GiB and 3.3 minutes, all-gather 0.2 GiB and 1.4
  # minutes, the e256 test 0.8 GiB and 1 minute, the e128 test 0.3 GiB and 13
  # seconds. Every other test together takes about 4 minutes at -j4 on four
  # dedicated cores, the runner's share. getdeps cannot pass ctest a label
  # filter, so the default has to be decided here.
  set(PACKSIM_HEAVY_TEST_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/oss_tests/all-gather-parallel-qp-test.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/oss_tests/all-reduce-parallel-qp-test.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/oss_tests/all-to-all-workload-rank-parallel-qp-e128-test.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/packsim/workloads/oss_tests/all-to-all-workload-rank-parallel-qp-e256-test.cpp"
  )
  packsim_require_present(
    PACKSIM_HEAVY_TEST_SOURCES ${PACKSIM_HEAVY_TEST_SOURCES})
  if(PACKSIM_HEAVY_TESTS)
    set(packsim_heavy_tests_disabled FALSE)
  else()
    set(packsim_heavy_tests_disabled TRUE)
    list(LENGTH PACKSIM_HEAVY_TEST_SOURCES packsim_heavy_test_count)
    message(STATUS "packsim: ${packsim_heavy_test_count} heavy tests are built "
      "but not run; -DPACKSIM_HEAVY_TESTS=ON runs them")
  endif()

  list(LENGTH packsim_test_sources packsim_test_count)
  message(STATUS "packsim: ${packsim_test_count} test executables")

  # Test executables are named from the bare basename, which is unique across
  # the export today but is not guaranteed to stay that way -- the sources are
  # globbed out of every `oss_tests/` directory in the tree, and two of them
  # could come to hold a `queue-test.cpp` each. CMake's own error for that
  # ("add_executable cannot create target ... already exists") names neither
  # source, so check it here where both can be printed. Reserved names are
  # checked too: a test source called `packsim.cpp` would collide with the
  # binary rather than with another test.
  set(packsim_reserved_target_names
    packsim packsim-replay coro_demo
    packsim_core packsim_thrift packsim_test_support packsim_thrift_codegen)
  # Two parallel lists rather than one: the basenames are what collide, but the
  # error has to name both FULL paths, which is the whole reason for checking
  # here instead of letting CMake's own duplicate-target error fire.
  set(packsim_seen_test_names "")
  set(packsim_seen_test_paths "")
  foreach(test_source IN LISTS packsim_test_sources)
    get_filename_component(test_name "${test_source}" NAME_WE)
    if("${test_name}" IN_LIST packsim_seen_test_names)
      list(FIND packsim_seen_test_names "${test_name}" packsim_first_index)
      list(GET packsim_seen_test_paths ${packsim_first_index} packsim_first_path)
      message(FATAL_ERROR
        "two test sources share the basename `${test_name}`, which would name "
        "the same CMake target twice. Rename one:\n"
        "  ${packsim_first_path}\n"
        "  ${test_source}")
    endif()
    if("${test_name}" IN_LIST packsim_reserved_target_names)
      message(FATAL_ERROR
        "test source `${test_source}` would create a target named "
        "`${test_name}`, which this build already uses. Rename the source.")
    endif()
    list(APPEND packsim_seen_test_names "${test_name}")
    list(APPEND packsim_seen_test_paths "${test_source}")
    add_executable("${test_name}" "${test_source}")
    target_link_libraries("${test_name}" PRIVATE packsim_test_support)
    add_test(NAME "${test_name}" COMMAND "${test_name}")
    # Without this a hung test has no bound below the CI job's own, so one
    # deadlock costs the whole run and reports as a job timeout naming nothing.
    # With it, ctest kills that test and the run continues, so the report names
    # the test.
    #
    # Deliberately generous rather than tuned. At -j4 on four dedicated cores,
    # standing in for the published CI's 4-vCPU runner, the slowest default test
    # takes about 100 s and all of them together about 4 minutes; on a loaded
    # workstation the slowest took 140 s. The workflow itself has never run,
    # and a tight bound would turn a slow pass into a red build, which is worse
    # than the problem. Lower it once a green run has reported real figures,
    # alongside `timeout_minutes` in the getdeps manifest.
    if(test_source IN_LIST PACKSIM_HEAVY_TEST_SOURCES)
      # DISABLED rather than left unregistered, so ctest lists them as
      # "Not Run (Disabled)" instead of dropping them silently.
      set_tests_properties("${test_name}" PROPERTIES
        LABELS heavy DISABLED ${packsim_heavy_tests_disabled} TIMEOUT 3600)
    else()
      set_tests_properties("${test_name}" PROPERTIES TIMEOUT 900)
    endif()
  endforeach()

  # End to end, which none of the unit tests above does: run the shipped
  # example with the default flags (replay snapshot, filesystem blob store,
  # JSON-lines event log), replay it, and check that both results are standard
  # JSON reporting the same delivered bytes. `string(JSON)` without an error
  # variable fails on a document that does not parse.
  set(packsim_e2e_dir "${CMAKE_CURRENT_BINARY_DIR}/packsim-e2e")
  file(MAKE_DIRECTORY "${packsim_e2e_dir}")
  file(WRITE "${packsim_e2e_dir}/check-json.cmake" [=[
foreach(f result.json replay.json)
  file(READ "${DIR}/${f}" json)
  string(JSON bytes GET "${json}" stats totalBytesDelivered)
  if(NOT bytes GREATER 0)
    message(FATAL_ERROR "${f}: stats.totalBytesDelivered is ${bytes}")
  endif()
  list(APPEND delivered "${bytes}")
endforeach()
list(GET delivered 0 run_bytes)
list(GET delivered 1 replay_bytes)
if(NOT run_bytes EQUAL replay_bytes)
  message(FATAL_ERROR "replay delivered ${replay_bytes} bytes, the run ${run_bytes}")
endif()
]=])
  add_test(NAME packsim-example-run COMMAND packsim
    "--json_cfg=${CMAKE_CURRENT_SOURCE_DIR}/packsim/examples/incast-2host.json"
    --output_file=result.json --run_id=ctest_e2e
    WORKING_DIRECTORY "${packsim_e2e_dir}")
  add_test(NAME packsim-example-replay COMMAND packsim-replay
    --run_id=ctest_e2e --output_file=replay.json
    WORKING_DIRECTORY "${packsim_e2e_dir}")
  add_test(NAME packsim-example-json COMMAND "${CMAKE_COMMAND}"
    "-DDIR=${packsim_e2e_dir}" -P "${packsim_e2e_dir}/check-json.cmake")
  set_tests_properties(packsim-example-run PROPERTIES
    FIXTURES_SETUP packsim_e2e_run TIMEOUT 120)
  set_tests_properties(packsim-example-replay PROPERTIES
    FIXTURES_REQUIRED packsim_e2e_run FIXTURES_SETUP packsim_e2e_replay
    TIMEOUT 120)
  set_tests_properties(packsim-example-json PROPERTIES
    FIXTURES_REQUIRED "packsim_e2e_run;packsim_e2e_replay" TIMEOUT 60)
endif()

# ---------------------------------------------------------------------------
# Source inventory
#
# The globs above track the export by themselves: a new source, a new test, even
# a new `oss_tests/` directory needs nothing here. The one thing they do not
# cover is the IDL list, which has to be enumerated because each file gets its
# own codegen step. Left to drift, a .thrift added to `packsim/if/` would simply
# not be generated, and the first symptom would be a missing header in some
# unrelated translation unit -- so it fails at configure time instead, naming
# both lists. That check is up beside `PACKSIM_THRIFT_FILES` itself, ahead of
# the codegen loop it has to beat; see the note there.
# ---------------------------------------------------------------------------
