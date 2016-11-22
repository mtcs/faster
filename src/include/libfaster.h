#ifndef LIBFASTER_H
#define LIBFASTER_H

#include "fdd.h"
#include "indexedFdd.h"
#include "groupedFdd.h"
#include "fastContext.h"

/// \mainpage Introdutory Pages
/// The context class is the class that manages dataset resources and task execution.
///
/// - faster::fastContext class
///
/// The user can create, using the context class several types of distributted datasets:
/// - faster::fdd - a dataset of a single type.
/// - faster::indexedFdd dataset - a indexed dataset containing a key and a value.
/// - faster::groupedFdd dataset class - a group of indexed datasets.
///
/// For working examples:
/// - \subpage examples Examples


/// \page examples Examples
/// Faster has full working examples at src/examples directory.
///
/// __Some toy examples:__
///
/// - fexample-int.cpp - A example applying map and reduce to a faster::fdd <int> created from memory
/// - fexample-int-file.cpp - A example applying map and reduce to a faster::fdd<int> created from file
/// - fexample-int-vector.cpp - A example applying map and reduce to a faster::fdd<vector<int>> created from memory
/// - fexample-indexed.cpp - A example applying map and reduce to a faster::indexedFdd<int,int> created from memory
///
/// __Some algorithm implementations using Faster:__
///
/// - pagerank.cpp - A pagerank implementation without using bulk functions
/// - pagerank-bulk2.cpp - A pagerank implementation without using bulk functions

/// @brief libfaster main namespace
namespace faster{
}

#endif
