find_package(GTest CONFIG QUIET)

if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.17.0
  )
  # For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

add_executable(slick-stream-buffer-multiplexer-tests
  multiplexer_tests.cpp
  multiplexer_mpmc_tests.cpp
  multiplexer_shm_tests.cpp
)
if(MSVC)
  add_compile_options(/wd4996)
endif()

target_link_libraries(slick-stream-buffer-multiplexer-tests PRIVATE slick::stream_buffer_multiplexer GTest::gtest_main)

# Loss detection is no longer a macro in any of the three libraries: it is a traits parameter, so
# the tests that assert on a counter pick it per instance (see multiplexer_test_support.hpp) and
# report the same numbers in Debug and Release. Passing the old -D here would only emit the
# deprecation warning each header raises for it.

include(GoogleTest)
gtest_discover_tests(slick-stream-buffer-multiplexer-tests)
