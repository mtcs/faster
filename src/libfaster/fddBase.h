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
		size_t getSize(){ return size; }
		int getId(){return id;}

		virtual void * _collect() = 0;
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

// Not Indexed FFDs
// FDD function pointer types

template <typename T, typename U>
using mapFunctionP = U (*) (T & input);
template <typename T, typename U>
using ImapFunctionP = U (*) (T & input);
template <typename T, typename U>
using PmapFunctionP = void (*) (U & output, size_t &outputSize, T & input);
template <typename T, typename U>
using IPmapFunctionP = void (*) (U & output, size_t &outputSize, T & input);

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

// Indexed FDDs
// FDD function pointer types
// TODO CHANGE THIS BELOW!
template <typename K, typename T, typename U>
using ImapIFunctionP = std::pair<K,U> (*) (K & key, T & input);
template <typename K, typename T, typename U>
using mapIFunctionP = U (*) (K & key, T & input);
template <typename K, typename T, typename U>
using IPmapIFunctionP = void (*) (U & output, size_t &outputSize, T & input);
template <typename K, typename T, typename U>
using PmapIFunctionP = void (*) (U & output, size_t &outputSize, T & input);

template <typename K, typename T, typename U>
using IbulkMapIFunctionP = void (*) (U * output, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using bulkMapIFunctionP = void (*) (U * output, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using IPbulkMapIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using PbulkMapIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * key, T * input, size_t size);

template <typename K, typename T, typename U>
using IflatMapIFunctionP = std::list<std::pair<K,U>> (*) (K & key, T & input);
template <typename K, typename T, typename U>
using flatMapIFunctionP = std::list<U> (*) (K & key, T & input);
template <typename K, typename T, typename U>
using IPflatMapIFunctionP = std::list<std::tuple<K, U,size_t>>  (*) (K & key, T & input);
template <typename K, typename T, typename U>
using PflatMapIFunctionP = std::list<std::pair<U, size_t>>  (*) (K & key, T & input);

template <typename K, typename T, typename U>
using IbulkFlatMapIFunctionP = void (*) (K *& outKey, U *& output, size_t & outputSize, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using bulkFlatMapIFunctionP = void (*) (U *& output, size_t & outputSize, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using IPbulkFlatMapIFunctionP = void (*) (K *& outKey,U *& output, size_t *& outputDataSizes, size_t & outputSize, K * key, T * input, size_t size);
template <typename K, typename T, typename U>
using PbulkFlatMapIFunctionP = void (*) (U *& output, size_t *& outputDataSizes, size_t & outputSize, K * key, T * input, size_t size);

template <typename K, typename T>
using IreduceIFunctionP = std::pair<K,T> (*) (K & keyA, T & a, K & keyB, T & b);

template <typename K, typename T>
using IbulkReduceIFunctionP = std::pair<K,T> (*) (K * key, T * input, size_t size);

// Pointer FDD function pointer types

// Map
template <typename K, typename T, typename U>
using ImapPIFunctionP = std::pair<K,U> (*) (K & key, T * input, size_t size);
template <typename K, typename T, typename U>
using  mapPIFunctionP = U (*) (T * input, K & key, size_t size);
template <typename K, typename T, typename U>
using IPmapPIFunctionP = void (*) (U & output, size_t & outputSize, K & key, T * input, size_t size);
template <typename K, typename T, typename U>
using  PmapPIFunctionP = void (*) (U & output, size_t & outputSize, K * key, T * input, size_t size);


template <typename K, typename T, typename U>
using IbulkMapPIFunctionP = void (*) (U * output, K * key, T ** input, size_t * inputDataSizes, size_t size);
template <typename K, typename T, typename U>
using  bulkMapPIFunctionP = void (*) (U * output, K * key, T ** input, size_t * inputDataSizes, size_t size);
template <typename K, typename T, typename U>
using IPbulkMapPIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * key, T ** input, size_t * inputDataSizes, size_t size);
template <typename K, typename T, typename U>
using  PbulkMapPIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * key, T ** input, size_t * inputDataSizes, size_t size);


template <typename K, typename T, typename U>
using IflatMapPIFunctionP = std::list<std::pair<K,U>> (*) (T *& input, size_t size);
template <typename K, typename T, typename U>
using flatMapPIFunctionP = std::list<std::pair<K,U>> (*) (T *& input, size_t size);
template <typename K, typename T, typename U>
using IPflatMapPIFunctionP = std::list<std::tuple<K, U, size_t>> (*) (T *& input, size_t size);
template <typename K, typename T, typename U>
using PflatMapPIFunctionP = std::list<std::pair<U, size_t>> (*) (T *& input, size_t size);


template <typename K, typename T, typename U>
using IbulkFlatMapPIFunctionP = void (*) (U *& output, size_t & outputSize, K * key, T ** input, size_t * inputDataSizes, size_t size) ;
template <typename K, typename T, typename U>
using bulkFlatMapPIFunctionP = void (*) (U *& output, size_t & outputSize, K * key, T ** input, size_t * inputDataSizes, size_t size) ;
template <typename K, typename T, typename U>
using IPbulkFlatMapPIFunctionP = void (*) (U *& output, size_t * outputDataSizes, size_t & outputSize, K * key, T ** input, size_t * inputDataSizes, size_t size);
template <typename K, typename T, typename U>
using PbulkFlatMapPIFunctionP = void (*) (U *& output, size_t * outputDataSizes, size_t & outputSize, K * key, T ** input, size_t * inputDataSizes, size_t size);


// Reduce
template <typename K, typename T>
using PIreducePIFunctionP = std::tuple<K,T*,size_t> (*) (K & keyA, T * a, size_t sizeA, K & keyB, T * b, size_t sizeB);

template <typename K, typename T>
using PIbulkReducePIFunctionP = std::tuple<K,T*,size_t> (*) (K * key, T ** input, size_t * inputDataSizes, size_t size);

#endif
