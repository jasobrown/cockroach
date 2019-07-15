# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# NB: Despite CMake's portability, this build configuration makes no attempt to
# support non-GCC-like compilers.

# The CXX_STANDARD property was introduced in version 3.1
# 3.3 fixes https://cmake.org/cmake/help/v3.3/policy/CMP0060.html
cmake_minimum_required(VERSION 3.3 FATAL_ERROR)

project(pmemroachlib)

add_library(pmemroach
  src/db.cc
  src/encoding.cc
  src/engine.cc
  src/godefs.cc
  src/iterator.cc
  src/merge.cc
  src/mvcc.cc
  # protos/roachpb/data.pb.cc
  # protos/roachpb/internal.pb.cc
  # protos/roachpb/metadata.pb.cc
  # protos/storage/engine/enginepb/mvcc.pb.cc
  # protos/storage/engine/enginepb/mvcc3.pb.cc
  # protos/util/hlc/legacy_timestamp.pb.cc
  # protos/util/hlc/timestamp.pb.cc
  # protos/util/log/log.pb.cc
  # protos/util/unresolved_addr.pb.cc
)
target_include_directories(pmemroach
  PUBLIC  ./include
  PRIVATE ../protobuf/src
  PRIVATE protos
  PRIVATE "/opt/dev/cpp/folly/build/default/opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/folly/include"
  PRIVATE "/opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/libpmemobj-cpp/include"
)

target_link_libraries(pmemroach
  pthread
  # would prefer to use my own boost rather than the system, but ...
#  /opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/boost/lib/libboost_context.a
#  /opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/boost/lib/libboost_thread.a
#  /opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/boost/lib/libboost_system.a
  /opt/dev/cpp/folly/build/default/opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/folly/lib/libfolly.a
  glog
  /usr/lib/x86_64-linux-gnu/libiberty.a
  /usr/lib/x86_64-linux-gnu/libdouble-conversion.a
  gflags
  dl
  atomic
  boost_context
  boost_thread
  boost_system
  event
  hwloc

  /opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/pmdk/src/nondebug/libpmem.so
  /opt/dev/cockroach/19.1/native/x86_64-pc-linux-gnu/pmdk/src/nondebug/libpmemobj.so
  ndctl
  daxctl
)

# TODO(jeb) maybe remove this for 'real' builds
set(CMAKE_BUILD_TYPE Debug)

set_target_properties(pmemroach PROPERTIES
  CXX_STANDARD 14
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
  COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare"
)

enable_testing()

# List of tests to build and run.
set(tests
  src/db_test.cc
  src/encoding_test.cc
  src/merge_test.cc
)

# "test" doesn't depend on the actual tests. Let's add a "check" target
# that depends on all test executables and runs "ctest".
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -V)

# Add googletest and get around a weird cmake issue:
# https://gitlab.kitware.com/cmake/cmake/issues/16920
set(THREADS_PTHREAD_ARG "2" CACHE STRING "Forcibly set by CMakeLists.txt." FORCE)
add_subdirectory(../googletest/googletest
                 ${CMAKE_BINARY_DIR}/googletest
                 EXCLUDE_FROM_ALL)

# TODO(benesch): make this required when CMake 3.9 is widely deployed.
include(GoogleTest OPTIONAL)

# Iterate over all test sources.
foreach(tsrc ${tests})
  # Build target name from filename
  get_filename_component(filename ${tsrc} NAME_WE)
  get_filename_component(dirname ${tsrc} DIRECTORY)
  if("${dirname}" STREQUAL "" )
    set(tname ${filename})
  else()
    set(tname ${dirname}_${filename})
  endif()

  # Use testutils.
  add_executable(${tname} ${tsrc} src/testutils.cc)

	# Set includes/libraries/properties.
  target_include_directories(${tname}
    PRIVATE ../googletest/googletest/include
    PRIVATE ../protobuf/src
    PRIVATE protos
  )

  # Add all other libraries.
  target_link_libraries(${tname}
    pmemroach
    gtest_main
    pthread
    ${PROTOBUF_LIB}
  )

  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    target_link_libraries(${tname} rt)
  endif()

  set_target_properties(${tname} PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
    COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare"
  )

  # Add the executable to the set of tests run by the "check" target.
  if(COMMAND gtest_discover_tests)
    # gtest_discover_tests, introduced in CMake 3.10, teaches CTest about the
    # actual test cases within the test binary.
    gtest_discover_tests(${tname})
  else()
    # In earlier versions, just tell CTest to treat the test binary as a black
    # box that returns an exit code.
    add_test(${tname} ${tname})
  endif()
  add_dependencies(check ${tname})
endforeach(tsrc)
