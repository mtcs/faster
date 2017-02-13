cmake_minimum_required(VERSION 2.8)

project (faster-tests)

set (faster_VERSION_MAJOR 0)
set (faster_VERSION_MINOR 1)

include_directories ("${PROJECT_SOURCE_DIR}/../libfaster")
set (EXTRA_LIBS ${EXTRA_LIBS} faster)

#set (EXTRA_LIBS ${EXTRA_LIBS} efence)
#set (EXTRA_LIBS ${EXTRA_LIBS} duma)

include ( CTest )

function(GTEST_ADD_MPI_TESTS executable extra_args)
	if(NOT ARGN)
		message(FATAL_ERROR "Missing ARGN: Read the documentation for MY_GTEST_ADD_TESTS")
	endif()
	if(ARGN STREQUAL "AUTO")
		# obtain sources used for building that executable
		get_property(ARGN TARGET ${executable} PROPERTY SOURCES)
	endif()
	set(gtest_case_name_regex ".*\\( *([A-Za-z_0-9]+) *, *([A-Za-z_0-9]+) *\\).*")
	set(gtest_test_type_regex "(TYPED_TEST|TEST_?[FP]?)")
	foreach(source ${ARGN})
		set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${source})
		file(READ "${source}" contents)
		string(REGEX MATCHALL "${gtest_test_type_regex} *\\(([A-Za-z_0-9 ,]+)\\)" found_tests ${contents})
		foreach(hit ${found_tests})
			string(REGEX MATCH "${gtest_test_type_regex}" test_type ${hit})

			# Parameterized tests have a different signature for the filter
			if("x${test_type}" STREQUAL "xTEST_P")
				string(REGEX REPLACE ${gtest_case_name_regex}  "*/\\1.\\2/*" test_name ${hit})
			elseif("x${test_type}" STREQUAL "xTEST_F" OR "x${test_type}" STREQUAL "xTEST")
				string(REGEX REPLACE ${gtest_case_name_regex} "\\1.\\2" test_name ${hit})
			elseif("x${test_type}" STREQUAL "xTYPED_TEST")
				string(REGEX REPLACE ${gtest_case_name_regex} "\\1/*.\\2" test_name ${hit})
			else()
				message(WARNING "Could not parse GTest ${hit} for adding to CTest.")
				continue()
			endif()
			add_test(NAME mpi-${test_name}
				WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
				COMMAND mpirun -np 4 ./tests/${executable} --gtest_filter=${test_name} ${extra_args})
			set_custom_env(mpi-${test_name})
		endforeach()
	endforeach()
endfunction(GTEST_ADD_MPI_TESTS)

function(GTEST_ADD_MY_TESTS executable extra_args)
	if(NOT ARGN)
		message(FATAL_ERROR "Missing ARGN: Read the documentation for MY_GTEST_ADD_TESTS")
	endif()
	if(ARGN STREQUAL "AUTO")
		# obtain sources used for building that executable
		get_property(ARGN TARGET ${executable} PROPERTY SOURCES)
	endif()
	set(gtest_case_name_regex ".*\\( *([A-Za-z_0-9]+) *, *([A-Za-z_0-9]+) *\\).*")
	set(gtest_test_type_regex "(TYPED_TEST|TEST_?[FP]?)")
	foreach(source ${ARGN})
		set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${source})
		file(READ "${source}" contents)
		string(REGEX MATCHALL "${gtest_test_type_regex} *\\(([A-Za-z_0-9 ,]+)\\)" found_tests ${contents})
		foreach(hit ${found_tests})
			string(REGEX MATCH "${gtest_test_type_regex}" test_type ${hit})

			# Parameterized tests have a different signature for the filter
			if("x${test_type}" STREQUAL "xTEST_P")
				string(REGEX REPLACE ${gtest_case_name_regex}  "*/\\1.\\2/*" test_name ${hit})
			elseif("x${test_type}" STREQUAL "xTEST_F" OR "x${test_type}" STREQUAL "xTEST")
				string(REGEX REPLACE ${gtest_case_name_regex} "\\1.\\2" test_name ${hit})
			elseif("x${test_type}" STREQUAL "xTYPED_TEST")
				string(REGEX REPLACE ${gtest_case_name_regex} "\\1/*.\\2" test_name ${hit})
			else()
				message(WARNING "Could not parse GTest ${hit} for adding to CTest.")
				continue()
			endif()
			add_test(NAME ${test_name}
				WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
				COMMAND ./tests/${executable} --gtest_filter=${test_name} ${extra_args})
			set_custom_env(${test_name})
		endforeach()
	endforeach()
endfunction(GTEST_ADD_MY_TESTS)


find_package(GTest)
if(GTEST_FOUND)
	message("GTest enabled")
	SET(CTEST_TESTING_TIMEOUT 50)
	SET(CTEST_OUTPUT_ON_FAILURE TRUE)
	add_definitions(${GTEST_COMPILE_FLAGS})
	include_directories(${GTEST_INCLUDE_DIRS} )

	# Coverage
	SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage" )
	SET( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov" )
	include(Dart)

	set (EXTRA_LIBS ${EXTRA_LIBS} ${GTEST_BOTH_LIBRARIES})

	find_package(MPI REQUIRED)
	#set (MPI_CXX_COMPILER "mpicc")
	add_definitions(${MPI_CXX_COMPILE_FLAGS})
	include_directories(${MPI_CXX_INCLUDE_PATH})
	link_directories(${MPI_CXX_LIBRARIES})
	#set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS})
	#set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS} ${MPI_CXX_LINK_FLAGS})
	#include_directories("/usr/include/openmpi/")
	set (EXTRA_LIBS ${MPI_CXX_LIBRARIES} ${EXTRA_LIBS})


	# Test for simple file
	function(dec_test test_name)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${EXTRA_LIBS})
		#add_test(${test_name} ${test_name})
		#set_custom_env(${test_name})
		set(${test_name}Args "0")
		GTEST_ADD_MY_TESTS(${test_name} ${test_name}Args AUTO)
	endfunction(dec_test)

	# Test for simple file with a selected library
	function(dec_test_select test_name libs)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${libs} ${EXTRA_LIBS})
		#target_link_libraries (${test_name}  ${libs} ${GTEST_BOTH_LIBRARIES})
		#add_test(${test_name} ${test_name})
		#set_custom_env(${test_name})
		set(${test_name}Args "0")
		GTEST_ADD_MY_TESTS(${test_name} ${test_name}Args AUTO)
	endfunction(dec_test_select)

	# Test for mpi dependent file
	function(mpi_test test_name)
		add_executable(${test_name} ${test_name}.cpp)
		target_link_libraries (${test_name}  ${EXTRA_LIBS})
		#add_test(NAME ${test_name}
		#	 WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
		##	 COMMAND mpirun -np 4 ./tests/${test_name}
		#	 )
		#set_custom_env(${test_name})
		set(${test_name}Args "0")
		GTEST_ADD_MPI_TESTS(${test_name} ${test_name}Args AUTO)
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
	if (hdfsEnabled)
		dec_test(gtest-hdfsEngine hdfsEngine)
	endif (hdfsEnabled)

else(GTEST_FOUND)
	message("GTest not found, disabling tests")
endif(GTEST_FOUND)


