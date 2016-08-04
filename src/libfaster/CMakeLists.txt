cmake_minimum_required(VERSION 2.8)

project(libfaster)


set (libfaster_VERSION_MAJOR 0)
set (libfaster_VERSION_MINOR 1)


set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ldl ")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(OpenMP)
if(OPENMP_FOUND)
 set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
 set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif(OPENMP_FOUND)

find_package(MPI REQUIRED)
#set (MPI_CXX_COMPILER "mpicc")
add_definitions(${MPI_CXX_COMPILE_FLAGS})
include_directories(${MPI_CXX_INCLUDE_PATH})
link_directories(${MPI_CXX_LIBRARIES})
#set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS})
#set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS} ${MPI_LINK_FLAGS})
#include_directories("/usr/include/openmpi/")
set (EXTRA_LIBS ${EXTRA_LIBS} ${MPI_CXX_LIBRARIES})


#set (MODULES ${MODULES} fdd.cpp ) # Pure templates now
#set (MODULES ${MODULES} indexedFdd.cpp ) # Pure templates now
set (MODULES ${MODULES} fastContext.cpp )
set (MODULES ${MODULES} fastScheduler.cpp )
#set (MODULES ${MODULES} fastComm.cpp ) to DL
#set (MODULES ${MODULES} fastCommBuffer.cpp ) to DL
set (MODULES ${MODULES} misc.cpp )
set (MODULES ${MODULES} worker.cpp )
set (MODULES ${MODULES} workerRun.cpp )
set (MODULES ${MODULES} workerCreate.cpp )
set (MODULES ${MODULES} workerICreate.cpp )
set (MODULES ${MODULES} workerFddBase.cpp )
set (MODULES ${MODULES} workerFddGroup0.cpp )
set (MODULES ${MODULES} workerFddGroup1.cpp )
set (MODULES ${MODULES} workerFddGroup2.cpp )
set (MODULES ${MODULES} workerFddGroup3.cpp )
set (MODULES ${MODULES} workerFddGroup4.cpp )
set (MODULES ${MODULES} workerFddGroup5.cpp )
set (MODULES ${MODULES} workerFddWrapper.cpp )
#set (MODULES ${MODULES} workerSFdd.cpp ) to DL
#set (MODULES ${MODULES} workerCFdd.cpp ) to DL
#set (MODULES ${MODULES} workerPFdd.cpp ) to DL
#set (MODULES ${MODULES} fddStorage.cpp ) to DL
#set (MODULES ${MODULES} indexedFddStorage.cpp )
#set (MODULES ${MODULES} workerFddCore.cpp )
#set (MODULES ${MODULES} workerIFdd.cpp )
#set (MODULES ${MODULES} workerIPFdd.cpp )
#set (MODULES ${MODULES} workerIFddCore.cpp )
#set (MODULES ${MODULES} workerIFddInstance0.cpp )
#set (MODULES ${MODULES} workerIFddInstance1.cpp )
#set (MODULES ${MODULES} workerIFddInstance2.cpp )
#set (MODULES ${MODULES} workerIFddInstance3.cpp )
#set (MODULES ${MODULES} workerIFddInstance4.cpp )
#set (MODULES ${MODULES} workerIFddInstance5.cpp )
#set (MODULES ${MODULES} workerIFddInstance6.cpp )
#set (MODULES ${MODULES} workerIFddInstance7.cpp )
#set (MODULES ${MODULES} workerIFddInstance8.cpp )
#set (MODULES ${MODULES} workerIFddInstance9.cpp )
#set (MODULES ${MODULES} workerIFddInstance10.cpp )
#set (MODULES ${MODULES} workerIFddInstance11.cpp )
#set (MODULES ${MODULES} workerIPFddInstance0.cpp )
#set (MODULES ${MODULES} workerIPFddInstance1.cpp )
#set (MODULES ${MODULES} workerIPFddInstance2.cpp )
#set (MODULES ${MODULES} workerIPFddInstance3.cpp )
#set (MODULES ${MODULES} workerIPFddInstance4.cpp )
#set (MODULES ${MODULES} workerIPFddInstance5.cpp )

set (HEADERS ${HEADERS} ../include/definitions.h  )
set (HEADERS ${HEADERS} ../include/fastComm.h  )
set (HEADERS ${HEADERS} ../include/fastCommBuffer.h)
set (HEADERS ${HEADERS} ../include/fastContext.h  )
set (HEADERS ${HEADERS} ../include/fastScheduler.h  )
set (HEADERS ${HEADERS} ../include/fastTask.h  )
set (HEADERS ${HEADERS} ../include/fdd.h  )
set (HEADERS ${HEADERS} ../include/fddBase.h  )
set (HEADERS ${HEADERS} ../include/fddStorage.h  )
set (HEADERS ${HEADERS} ../include/fddStorageBase.h  )
set (HEADERS ${HEADERS} ../include/indexedFdd.h  )
set (HEADERS ${HEADERS} ../include/indexedFddStorage.h  )
set (HEADERS ${HEADERS} ../include/libfaster.h  )
set (HEADERS ${HEADERS} ../include/misc.h  )
set (HEADERS ${HEADERS} ../include/worker.h  )
set (HEADERS ${HEADERS} ../include/workerFddBase.h  )
set (HEADERS ${HEADERS} ../include/workerFdd.h  )
set (HEADERS ${HEADERS} ../include/workerIFdd.h)

set(commonLibSources fastComm.cpp fastCommBuffer.cpp)
if (hdfsEnabled)
	set( commonLibSources ${commonLibSources} hdfsEngine.cpp )
endif (hdfsEnabled)

# Common Dynamic LINKED libs
add_library( fasterCommon SHARED ${commonLibSources} )
set ( COMMDLIBS  fasterCommon )

# Worker Side Dynamic LINKED libs
add_library(fasterWorkerFddCore SHARED workerFddCore.cpp fddStorage.cpp )
set ( COMMDLLIBS fasterWorkerFddCore )

add_library(fasterWorkerIFddCore SHARED workerIFddCore.cpp indexedFddStorage.cpp workerIFdd.cpp )
#add_library(fasterWorkerIPFddCore SHARED workerIFddCore.cpp indexedFddStorage.cpp workerIPFdd.cpp )
set ( ICOMMDLLIBS fasterWorkerIFddCore )
#set ( IPCOMMDLLIBS fasterWorkerIPFddCore )

# Dynamic LOADED libs
add_library(fasterWorkerSFdd MODULE workerSFdd.cpp)
add_library(fasterWorkerCFdd MODULE workerCFdd.cpp)
#add_library(fasterWorkerPFdd MODULE workerPFdd.cpp)

add_library(fasterWorkerIFddInstance0  MODULE workerIFddInstance0.cpp )
add_library(fasterWorkerIFddInstance1  MODULE workerIFddInstance1.cpp )
add_library(fasterWorkerIFddInstance2  MODULE workerIFddInstance2.cpp )
add_library(fasterWorkerIFddInstance3  MODULE workerIFddInstance3.cpp )
add_library(fasterWorkerIFddInstance4  MODULE workerIFddInstance4.cpp )
add_library(fasterWorkerIFddInstance5  MODULE workerIFddInstance5.cpp )
add_library(fasterWorkerIFddInstance6  MODULE workerIFddInstance6.cpp )
add_library(fasterWorkerIFddInstance7  MODULE workerIFddInstance7.cpp )
add_library(fasterWorkerIFddInstance8  MODULE workerIFddInstance8.cpp )
add_library(fasterWorkerIFddInstance9  MODULE workerIFddInstance9.cpp )
add_library(fasterWorkerIFddInstance10 MODULE workerIFddInstance10.cpp )
add_library(fasterWorkerIFddInstance11 MODULE workerIFddInstance11.cpp )

#add_library(fasterWorkerIPFddInstance0 MODULE workerIPFddInstance0.cpp )
#add_library(fasterWorkerIPFddInstance1 MODULE workerIPFddInstance1.cpp )
#add_library(fasterWorkerIPFddInstance2 MODULE workerIPFddInstance2.cpp )
#add_library(fasterWorkerIPFddInstance3 MODULE workerIPFddInstance3.cpp )
#add_library(fasterWorkerIPFddInstance4 MODULE workerIPFddInstance4.cpp )
#add_library(fasterWorkerIPFddInstance5 MODULE workerIPFddInstance5.cpp )

# Interface lib
add_library(faster SHARED ${MODULES})


# Linking libraries/modules

target_link_libraries(fasterWorkerSFdd ${COMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerCFdd ${COMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerPFdd ${COMMDLLIBS} ${COMMDLIBS})

target_link_libraries(fasterWorkerIFddInstance0   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance1   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance2   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance3   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance4   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance5   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance6   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance7   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance8   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance9   ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance10  ${ICOMMDLLIBS} ${COMMDLIBS})
target_link_libraries(fasterWorkerIFddInstance11  ${ICOMMDLLIBS} ${COMMDLIBS})

#target_link_libraries(fasterWorkerIPFddInstance0  ${IPCOMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerIPFddInstance1  ${IPCOMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerIPFddInstance2  ${IPCOMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerIPFddInstance3  ${IPCOMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerIPFddInstance4  ${IPCOMMDLLIBS} ${COMMDLIBS})
#target_link_libraries(fasterWorkerIPFddInstance5  ${IPCOMMDLLIBS} ${COMMDLIBS})

if (hdfsEnabled)
	target_link_libraries(fasterCommon ${JAVALIBS} ${HDFSLIBS} ${EXTRA_LIBS})
endif (hdfsEnabled)
target_link_libraries (faster ${EXTRA_LIBS} ${COMMDLIBS})

#cotire(fasterWorkerSFdd)
#cotire(fasterWorkerCFdd)
##cotire(fasterWorkerPFdd)

#cotire(fasterWorkerIFddInstance0 )
#cotire(fasterWorkerIFddInstance1 )
#cotire(fasterWorkerIFddInstance2 )
#cotire(fasterWorkerIFddInstance3 )
#cotire(fasterWorkerIFddInstance4 )
#cotire(fasterWorkerIFddInstance5 )
#cotire(fasterWorkerIFddInstance6 )
#cotire(fasterWorkerIFddInstance7 )
#cotire(fasterWorkerIFddInstance8 )
#cotire(fasterWorkerIFddInstance9 )
#cotire(fasterWorkerIFddInstance10)
#cotire(fasterWorkerIFddInstance11)

##cotire(fasterWorkerIPFddInstance0)
##cotire(fasterWorkerIPFddInstance1)
##cotire(fasterWorkerIPFddInstance2)
##cotire(fasterWorkerIPFddInstance3)
##cotire(fasterWorkerIPFddInstance4)
##cotire(fasterWorkerIPFddInstance5)

#cotire(faster)

#install(TARGETS libfaster DESTINATION lib)
#install(FILES ${HEADERS} DESTINATION include)
