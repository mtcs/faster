#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H

#include <list>
#include <vector>
#include <cstdlib>

enum fddType{
	Null,
	Char,
	Int,
	LongInt,
	Float,
	Double,
	Custom,
	String,
	CharP,
	IntP,
	LongIntP,
	FloatP,
	DoubleP,
};

enum fddOpType{
	Map,
	BulkMap,
	FlatMap,
	BulkFlatMap,
	Reduce,
	BulkReduce
};


class fddBase{
	protected:
		fddType type;
		unsigned long int id;
		unsigned long int totalBlocks;
		unsigned long int size;
	public:
		void setSize(size_t &s){ size = s; }
};


class fastTask{
	public:
		unsigned long int id;
		unsigned long int srcFDD;
		unsigned long int destFDD;
		fddOpType	operationType;
		unsigned int	functionId;
		void * result;
		size_t resultSize;
		size_t workersFinished;
};

// FDD function pointer types

template <typename T, typename U>
using mapFunctionP = U (*) (T & input);
template <typename T, typename U>
using PmapFunctionP = void (*) (U & output, size_t &outputSize, T & input);

template <typename T, typename U>
using bulkMapFunctionP = void (*) (U * output, T * input, size_t size);
template <typename T, typename U>
using PbulkMapFunctionP = void (*) (U * output, size_t * outputDataSizes, T * input, size_t size);

template <typename T, typename U>
using flatMapFunctionP = std::list<U> (*) (T & input);
template <typename T, typename U>
using PflatMapFunctionP = std::list<std::pair<U,size_t>>  (*) (T & input);

template <typename T, typename U>
using bulkFlatMapFunctionP = void (*) (U *& output, size_t & outputSize, T * input, size_t size);
template <typename T, typename U>
using PbulkFlatMapFunctionP = void (*) (U *& output, size_t *& outputDataSizes, size_t & outputSize, T * input, size_t size);

template <typename T>
using reduceFunctionP = T (*) (T & a, T & b);

template <typename T>
using bulkReduceFunctionP = T (*) (T * input, size_t size);

// Pointer FDD function pointer types

// Map
template <typename T, typename U>
using mapPFunctionP = U (*) (T * input, size_t size);
template <typename T, typename U>
using PmapPFunctionP = void (*) (U & output, size_t & outputSize, T * input, size_t size);


template <typename T, typename U>
using bulkMapPFunctionP = void (*) (U * output, T ** input, size_t * inputDataSizes, size_t size);
template <typename T, typename U>
using PbulkMapPFunctionP = void (*) (U * output, size_t * outputDataSizes, T ** input, size_t * inputDataSizes, size_t size);


template <typename T, typename U>
using flatMapPFunctionP = std::list<U> (*) (T *& input, size_t size);
template <typename T, typename U>
using PflatMapPFunctionP = std::list<std::pair<U, size_t>> (*) (T *& input, size_t size);


template <typename T, typename U>
using bulkFlatMapPFunctionP = void (*) (U *& output, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size) ;
template <typename T, typename U>
using PbulkFlatMapPFunctionP = void (*) (U *& output, size_t * outputDataSizes, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size);


// Reduce
template <typename T>
using PreducePFunctionP = void (*) (T *& output, size_t & outputDataSizes, T * a, size_t sizeA, T * b, size_t sizeB);


template <typename T>
using PbulkReducePFunctionP = void (*) (T *& output, size_t & outputDataSizes, T ** input, size_t * inputDataSizes, size_t size);

#endif
