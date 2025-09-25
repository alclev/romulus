cmake_minimum_required(VERSION 3.10)

find_package(GTest REQUIRED)

file(GLOB TEST_SOURCES *.cc)

foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_NAME}_test ${TEST_SRC})

  target_link_libraries(${TEST_NAME}_test
    PRIVATE
      GTest::GTest
      GTest::Main
      romulus             
      rdmacm           
      ibverbs
      pthread
  )
  add_test(NAME ${TEST_NAME}_test COMMAND ${TEST_NAME}_test)
endforeach()
