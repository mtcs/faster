#ifndef LIBFASTER_DEFINITIONS_H
#define LIBFASTER_DEFINITIONS_H

#include <list>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstdlib>

#define UNUSED __attribute__((unused))

namespace faster{


	typedef unsigned int fddType;

	#define Null		0x0000
	#define Char		0x0001
	#define Int		0x0002
	#define LongInt		0x0004
	#define Float		0x0008
	#define Double		0x0010
	#define String		0x0020
	#define POINTER		0x0100
	#define CharP		0x0101
	#define IntP		0x0102
	#define LongIntP	0x0104
	#define FloatP		0x0108
	#define DoubleP		0x0110
	#define Custom		0x0120
	#define VECTOR		0x1000
	#define CharV		0x1001
	#define IntV		0x1002
	#define LongIntV	0x1004
	#define FloatV		0x1008
	#define DoubleV		0x1010

	typedef unsigned int fddOpType;

	#define OP_GENERICMAP		0x0100
	#define OP_Map			0x0101
	#define OP_BulkMap		0x0102
	#define OP_FlatMap		0x0104
	#define OP_BulkFlatMap		0x0108
	#define OP_GENERICREDUCE	0x0200
	#define OP_Reduce		0x0201
	#define OP_BulkReduce		0x0202
	#define OP_CountByKey		0x0001
	#define OP_GroupByKey		0x0002

	typedef enum : char {
		NewWorkerDL,
		NewWorkerSDL,
		DestroyWorkerDL,

		GetTypeDL,
		GetKeyTypeDL,

		SetDataDL,
		SetDataRawDL,

		GetLineSizesDL,

		GetFddItemDL,
		GetDataDL,
		GetSizeDL,
		ItemSizeDL,
		BaseSizeDL,
		DeleteItemDL,
		ShrinkDL,

		InsertDL,
		InsertListDL,

		ApplyDL,

		CollectDL,
		GroupByKeyDL,
		CountByKeyDL,

	} dFuncName;

	// Not Indexed FFDs
	// FDD function pointer types

	template <typename T, typename U>
	using mapFunctionP = U (*) (T & input);
	template <typename T, typename L, typename U>
	using ImapFunctionP = std::pair<L,U> (*) (T & input);
	template <typename T, typename U>
	using PmapFunctionP = std::pair<U,size_t> (*) (T & input);
	template <typename T, typename L, typename U>
	using IPmapFunctionP = std::tuple<L,U,size_t> (*) (T & input);

	template <typename T, typename U>
	using bulkMapFunctionP = void (*) (U * output, T * input, size_t size);
	template <typename T, typename L, typename U>
	using IbulkMapFunctionP = void (*) (L * outKey, U * output, T * input, size_t size);
	template <typename T, typename U>
	using PbulkMapFunctionP = void (*) (U * output, size_t * outputDataSizes, T * input, size_t size);
	template <typename T, typename L, typename U>
	using IPbulkMapFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, T * input, size_t size);

	template <typename T, typename U>
	using flatMapFunctionP = std::list<U> (*) (T & input);
	template <typename T, typename L, typename U>
	using IflatMapFunctionP = std::list<std::pair<L,U>> (*) (T & input);
	template <typename T, typename U>
	using PflatMapFunctionP = std::list<std::pair<U,size_t>>  (*) (T & input);
	template <typename T, typename L, typename U>
	using IPflatMapFunctionP = std::list<std::tuple<L, U,size_t>>  (*) (T & input);

	template <typename T, typename U>
	using bulkFlatMapFunctionP = void (*) (U *& output, size_t & outputSize, T * input, size_t size);
	template <typename T, typename L, typename U>
	using IbulkFlatMapFunctionP = void (*) (L *& outKey, U *& output, size_t & outputSize, T * input, size_t size);
	template <typename T, typename U>
	using PbulkFlatMapFunctionP = void (*) (U *& output, size_t *& outputDataSizes, size_t & outputSize, T * input, size_t size);
	template <typename T, typename L, typename U>
	using IPbulkFlatMapFunctionP = void (*) (L *& outKey, U *& output, size_t *& outputDataSizes, size_t & outputSize, T * input, size_t size);

	template <typename T>
	using reduceFunctionP = T (*) (T & a, T & b);

	template <typename T>
	using bulkReduceFunctionP = T (*) (T * input, size_t size);

	// Pointer FDD function pointer types

	// Map
	template <typename T, typename U>
	using mapPFunctionP = U (*) (T * input, size_t size);
	template <typename T, typename L, typename U>
	using ImapPFunctionP = std::pair<L,U> (*) (T * input, size_t size);
	template <typename T, typename U>
	using PmapPFunctionP = std::pair<U,size_t> (*) (T * input, size_t size);
	template <typename T, typename L, typename U>
	using IPmapPFunctionP = std::tuple<L,U,size_t> (*) (T * input, size_t size);


	template <typename T, typename U>
	using bulkMapPFunctionP = void (*) (U * output, T ** input, size_t * inputDataSizes, size_t size);
	template <typename T, typename L, typename U>
	using IbulkMapPFunctionP = void (*) (L * outKey, U * output, T ** input, size_t * inputDataSizes, size_t size);
	template <typename T, typename U>
	using PbulkMapPFunctionP = void (*) (U * output, size_t * outputDataSizes, T ** input, size_t * inputDataSizes, size_t size);
	template <typename T, typename L, typename U>
	using IPbulkMapPFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, T ** input, size_t * inputDataSizes, size_t size);


	template <typename T, typename U>
	using flatMapPFunctionP = std::list<U> (*) (T *& input, size_t size);
	template <typename T, typename L, typename U>
	using IflatMapPFunctionP = std::list<std::pair<L,U>> (*) (T *& input, size_t size);
	template <typename T, typename U>
	using PflatMapPFunctionP = std::list<std::pair<U, size_t>> (*) (T *& input, size_t size);
	template <typename T, typename L, typename U>
	using IPflatMapPFunctionP = std::list<std::tuple<L, U, size_t>> (*) (T *& input, size_t size);


	template <typename T, typename U>
	using bulkFlatMapPFunctionP = void (*) (U *& output, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size) ;
	template <typename T, typename L, typename U>
	using IbulkFlatMapPFunctionP = void (*) (L *& outKey, U *& output, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size) ;
	template <typename T, typename U>
	using PbulkFlatMapPFunctionP = void (*) (U *& output, size_t * outputDataSizes, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size);
	template <typename T, typename L, typename U>
	using IPbulkFlatMapPFunctionP = void (*) (L *& outKey, U *& output, size_t * outputDataSizes, size_t & outputSize, T ** input, size_t * inputDataSizes, size_t size);


	// Reduce
	template <typename T>
	using PreducePFunctionP = std::pair<T *, size_t> (*) (T * a, size_t sizeA, T * b, size_t sizeB);


	template <typename T>
	using PbulkReducePFunctionP = std::pair<T *, size_t> (*) (T ** input, size_t * inputDataSizes, size_t size);

	// Indexed FDDs
	// FDD function pointer types
	// TODO CHANGE THIS BELOW!
	template <typename K, typename T, typename L, typename U>
	using ImapIFunctionP = std::pair<L,U> (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using mapIFunctionP = U (*) (K inKey, T & input);
	template <typename K, typename T, typename L, typename U>
	//using IPmapIFunctionP = void (*) (L & outKey, U & output, size_t &outputSize, T & input);
	using IPmapIFunctionP = std::tuple<L,U,size_t> (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using PmapIFunctionP = std::pair<U, size_t> (*) (K inKey, T & input);

	template <typename K, typename T, typename L, typename U>
	using IbulkMapIFunctionP = void (*) (L * outKey, U * output, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using bulkMapIFunctionP = void (*) (U * output, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkMapIFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using PbulkMapIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * inKey, T * input, size_t size);

	template <typename K, typename T, typename L, typename U>
	using IflatMapIFunctionP = std::list<std::pair<L,U>> (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using flatMapIFunctionP = std::list<U> (*) (K inKey, T & input);
	template <typename K, typename T, typename L, typename U>
	using IPflatMapIFunctionP = std::list<std::tuple<L, U,size_t>>  (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using PflatMapIFunctionP = std::list<std::pair<U, size_t>>  (*) (K inKey, T & input);

	template <typename K, typename T, typename L, typename U>
	using IbulkFlatMapIFunctionP = void (*) (L *& outKey, U *& output, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using bulkFlatMapIFunctionP = void (*) (U *& output, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkFlatMapIFunctionP = void (*) (L *& outKey, U *& output, size_t *& outputDataSizes, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using PbulkFlatMapIFunctionP = void (*) (U *& output, size_t *& outputDataSizes, size_t & outputSize, K * inKey, T * input, size_t size);

	template <typename K, typename T>
	using IreduceIFunctionP = std::pair<K,T> (*) (K keyA, T & a, K keyB, T & b);

	template <typename K, typename T>
	using IbulkReduceIFunctionP = std::pair<K,T> (*) (K * key, T * input, size_t size);

	// Pointer FDD function pointer types

	// Map
	template <typename K, typename T, typename L, typename U>
	using ImapIPFunctionP = std::pair<L,U> (*) (K inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using  mapIPFunctionP = U (*) (K inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPmapIPFunctionP = std::tuple<L,U,size_t> (*) (K inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using  PmapIPFunctionP = std::pair<U, size_t> (*) (K inKey, T * input, size_t size);


	template <typename K, typename T, typename L, typename U>
	using IbulkMapIPFunctionP = void (*) (L * outKey, U * output, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename U>
	using  bulkMapIPFunctionP = void (*) (U * output, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkMapIPFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename U>
	using  PbulkMapIPFunctionP = void (*) (U * output, size_t * outputDataSizes, K * inKey, T ** input, size_t * inputDataSizes, size_t size);


	template <typename K, typename T, typename L, typename U>
	using IflatMapIPFunctionP = std::list<std::pair<L,U>> (*) (T *& input, size_t size);
	template <typename K, typename T, typename U>
	using flatMapIPFunctionP = std::list<U> (*) (T *& input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPflatMapIPFunctionP = std::list<std::tuple<L, U, size_t>> (*) (T *& input, size_t size);
	template <typename K, typename T, typename U>
	using PflatMapIPFunctionP = std::list<std::pair<U, size_t>> (*) (T *& input, size_t size);


	template <typename K, typename T, typename L, typename U>
	using IbulkFlatMapIPFunctionP = void (*) (L *& outKey, U *& output, size_t & outputSize, K * inKey, T ** input, size_t * inputDataSizes, size_t size) ;
	template <typename K, typename T, typename U>
	using bulkFlatMapIPFunctionP = void (*) (U *& output, size_t & outputSize, K * inKey, T ** input, size_t * inputDataSizes, size_t size) ;
	template <typename K, typename T, typename L, typename U>
	using IPbulkFlatMapIPFunctionP = void (*) (L *& outKey, U *& output, size_t * outputDataSizes, size_t & outputSize, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename U>
	using PbulkFlatMapIPFunctionP = void (*) (U *& output, size_t * outputDataSizes, size_t & outputSize, K * inKey, T ** input, size_t * inputDataSizes, size_t size);


	// Reduce
	template <typename K, typename T>
	using IPreduceIPFunctionP = std::tuple<K,T*,size_t> (*) (K keyA, T * a, size_t sizeA, K keyB, T * b, size_t sizeB);

	template <typename K, typename T>
	using IPbulkReduceIPFunctionP = std::tuple<K,T*,size_t> (*) (K * key, T ** input, size_t * inputDataSizes, size_t size);




	// DATA TYPES
	template <typename K>
	using CountKeyMapT = std::unordered_map<K, size_t> ;

	template <typename K>
	using PPCountKeyMapT = std::unordered_map<K, std::pair<size_t, std::list<int>> > ;


}
#endif
