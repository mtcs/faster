cmake_minimum_required(VERSION 2.8)

project (faster-tests)

set (faster_VERSION_MAJOR 0)
set (faster_VERSION_MINOR 1)

include_directories ("${PROJECT_SOURCE_DIR}/../libfaster")
set (EXTRA_LIBS ${EXTRA_LIBS} faster)

#set (EXTRA_LIBS ${EXTRA_LIBS} efence)
#set (EXTRA_LIBS ${EXTRA_LIBS} duma)

find_package(GTest REQUIRED)
if(GTEST_FOUND)
	SET(CTEST_OUTPUT_ON_FAILURE TRUE)
	include_directories(${GTEST_INCLUDE_DIRS} )
	set (EXTRA_LIBS ${EXTRA_LIBS} ${GTEST_BOTH_LIBRARIES})

	function(dec_test test_name)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${EXTRA_LIBS})
		add_test(${test_name} ${test_name})
	endfunction(dec_test)

	function(dec_test_select test_name libs)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${EXTRA_LIBS} ${libs})
		add_test(${test_name} ${test_name})
	endfunction(dec_test_select)

	function(mpi_test test_name)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${EXTRA_LIBS})
		add_test(NAME ${test_name}
			WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
			COMMAND mpirun -np 4 ./tests/${test_name}
			)
	endfunction(mpi_test)


	# MPI independent tests
	dec_test(gtest-fastCommBuffer)
	dec_test(gtest-fastScheduler)
	dec_test_select(gtest-fddStorage fasterWorkerFddCore)
	dec_test_select(gtest-indexedFddStorage fasterWorkerIFddCore)
	dec_test(gtest-hasher)
	dec_test(gtest-misc)
	dec_test(gtest-_workerFdd)
	dec_test(gtest-_workerIFdd)

	# MPI dependent tests
	mpi_test(gtest-fastComm)
	mpi_test(gtest-fastContext)
	mpi_test(gtest-worker)
	mpi_test(gtest-fdd)
	mpi_test(gtest-indexedFdd)
	mpi_test(gtest-groupedFdd)

	# HDFS
	set (HDFSLIBS -L/usr/lib/hadoop/lib/native -lhdfs)
	set (JAVALIBS -L/usr/lib/jvm/default/jre/lib/amd64/server -ljvm)
	include_directories("/usr/lib/hadoop/include")
	add_executable(gtest-hdfsEngine gtest-hdfsEngine.cpp)
	target_link_libraries (gtest-hdfsEngine ${JAVALIBS} ${HDFSLIBS} ${EXTRA_LIBS})
	add_test(gtest-hdfsEngine gtest-hdfsEngine)

endif(GTEST_FOUND)


