cmake_minimum_required(VERSION 2.8)

project (faster)

set (faster_VERSION_MAJOR 0)
set (faster_VERSION_MINOR 1)

set (CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/CMake")
#include(cotire)

set( CMAKE_EXPORT_COMPILE_COMMANDS 1 )

# Options and user variables
option(hdfsEnabled 	"Haddop File System (HDFS) support" ON)
#option(offloadingEnabled "Accelerator Offloading support" OFF)
option(sanEnabled 	"Sanitizers debugging support" OFF)
option(examplesAsTests 	"Use examples as tests" OFF)
#option(dashboardSubmit 	"Submit tests to public dashboard" OFF)
set(hadoopHome  	"/usr/lib/hadoop" CACHE STRING "Haddop File System (HDFS) home directory")
set(jvmHome  		"/usr/lib/jvm/default" CACHE STRING "JVM home directory (for HDFS)")
option(cudaEnabled "CUDA GPU acceleration support" OFF)
set(cudaKitRoot "CUDA Dev Kit root dir" CACHE STRING "/opt/cuda/")

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if (CMAKE_BUILD_TYPE MATCHES "DEBUG")
	message("----------- DEBUG MODE -----------")
	add_definitions(-O0)
	add_definitions(-g3)
	#add_definitions(-g)
	add_definitions(-fstack-protector-all)
	add_definitions(-Wall)
	add_definitions(-Wextra)
	add_definitions(-Warray-bounds)
	add_definitions(-fno-omit-frame-pointer)

	# Coverage Tests
	if (NOT cudaEnabled)
		SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage" )
		SET( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov" )
	endif (NOT cudaEnabled)
	find_program(CTEST_MEMORYCHECK_COMMAND NAMES valgrind)
	include(Dart)

	if (NOT hdfsEnabled)
		if (sanEnabled)
			add_definitions(-fsanitize=address)
			#add_definitions(-fsanitize=thread)
			#add_definitions(-fsanitize=undefined)
			add_definitions(-fsanitize=leak)
			add_definitions(-fno-omit-frame-pointer)

			#set (EXTRA_LIBS tsan ${EXTRA_LIBS})
			#set (EXTRA_LIBS usan ${EXTRA_LIBS})
			set (EXTRA_LIBS lsan ${EXTRA_LIBS})
			set (EXTRA_LIBS asan ${EXTRA_LIBS})
			#set (EXTRA_LIBS duma ${EXTRA_LIBS})
			set (EXTRA_LIBS ${EXTRA_LIBS} util)

		endif (sanEnabled)
	endif (NOT hdfsEnabled)

	#if (dashboardSubmit)
	#endif (dashboardSubmit)

	# GPerfTools (NOT WORKING)
	#set (EXTRA_LIBS profiler ${EXTRA_LIBS})


else (CMAKE_BUILD_TYPE MATCHES "DEBUG")
	message("----------- RELEASE MODE -----------")
	add_definitions(-O3)
	#add_definitions(-g)
	add_definitions(-fomit-frame-pointer)
	add_definitions(-freorder-functions)
	#add_definitions(-pg)
	#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
	#set (EXTRA_LIBS ${EXTRA_LIBS} tcmalloc)
endif (CMAKE_BUILD_TYPE MATCHES "DEBUG")

if (cudaEnabled)
	message("CUDA Enabled")
	SET(CUDA_TOOLKIT_ROOT_DIR=cudaKitRoot)
	SET(CUDA_HOST_COMPILER "${CUDA_TOOLKIT_ROOT_DIR}/bin/nvcc")
endif (cudaEnabled)

#if (offloadingEnabled)
#add_definitions(-foffload=nvptx-none="-v")
#add_definitions(--with-build-time-tools=/usr/local/nvptx-none/bin)
#endif (offloadingEnabled)


if (hdfsEnabled)
	set (HDFSLIBS -L${hadoopHome}/lib/native -lhdfs)
	set (JAVALIBS -L${jvmHome}/jre/lib/amd64/server -ljvm)

	include_directories("${hadoopHome}/include")

	execute_process(COMMAND hadoop classpath --glob
		OUTPUT_VARIABLE GCLASSPATH)
	set (ENV{LD_LIBRARY_PATH} "${hadoopHome}/lib/native:${jvmHome}/jre/lib/amd64/server/")
	set (ENV{CLASSPATH} "${GCLASSPATH}")
endif (hdfsEnabled)


function(set_custom_env test_name)
	if (hdfsEnabled)
		set_property( TEST ${test_name}
			PROPERTY
			ENVIRONMENT "CLASSPATH=${GCLASSPATH}"
			)
		set_property( TEST ${test_name}
			APPEND
			PROPERTY
			ENVIRONMENT "LD_LIBRARY_PATH=${hadoopHome}/lib/native:${jvmHome}/jre/lib/amd64/server/"
			)
	else (hdfsEnabled)
		if (sanEnabled)
			set_property( TEST ${test_name}
				PROPERTY
				ENVIRONMENT "LD_PRELOAD=libasan.so.3:liblsan.so.0"
				)
			set_property( TEST ${test_name}
				APPEND
				PROPERTY
				ENVIRONMENT "ASAN_OPTIONS=symbolize=1:abort_on_error=1:verbosity=1"
				)
			set_property( TEST ${test_name}
				APPEND
				PROPERTY
				ENVIRONMENT "ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer"
				)
		endif (sanEnabled)
	endif (hdfsEnabled)
endfunction(set_custom_env)

find_package(OpenMP REQUIRED)
if(OPENMP_FOUND)
 	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
 	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
 	set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif(OPENMP_FOUND)

#find_package(MPI REQUIRED)
#set (MPI_CXX_COMPILER "mpicc")
#add_definitions(${MPI_CXX_COMPILE_FLAGS})
#include_directories(${MPI_CXX_INCLUDE_PATH})
#link_directories(${MPI_CXX_LIBRARIES})
#set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS})
#set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS} ${MPI_LINK_FLAGS})
#include_directories("/usr/include/openmpi/")
#set (EXTRA_LIBS ${EXTRA_LIBS} ${MPI_CXX_LIBRARIES})

enable_testing()

include_directories ("${PROJECT_SOURCE_DIR}/include")


add_subdirectory (libfaster)
add_subdirectory (test)
add_subdirectory (tests)
add_subdirectory (examples)


