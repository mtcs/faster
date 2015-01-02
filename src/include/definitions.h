#ifndef LIBFASTER_DEFINITIONS_H
#define LIBFASTER_DEFINITIONS_H

#include <list>
#include <deque>
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
	#define GroupFDD	0x8000

	typedef unsigned int fddOpType;

	#define OP_GENERICMAP		0x0100
	#define OP_Map			0x0101
	#define OP_BulkMap		0x0102
	#define OP_FlatMap		0x0104
	#define OP_BulkFlatMap		0x0108
	#define OP_MapByKey		0x0110
	#define OP_FlatMapByKey		0x0120
	#define OP_GENERICREDUCE	0x0200
	#define OP_Reduce		0x0201
	#define OP_BulkReduce		0x0202
	#define OP_GENERICUPDATE	0x0400
	#define OP_UpdateByKey		0x0401
	#define OP_BulkUpdate		0x0402
	#define OP_GENERICMISC		0x0800
	#define OP_CountByKey		0x0801
	#define OP_GroupByKey		0x0802
	#define OP_CoGroup		0x0804
	#define OP_Calibrate		0x0808

	typedef enum : char {
		NewWorkerDL 	= 0x01,
		NewWorkerSDL	= 0x02,
		DiscardWorkerDL	= 0x03,

		GetTypeDL	= 0x04,
		GetKeyTypeDL	= 0x05,

		SetDataDL	= 0x06,
		SetDataRawDL	= 0x07,

		GetLineSizesDL	= 0x08,

		GetFddItemDL	= 0x09,
		GetKeysDL	= 0x0a,
		GetDataDL	= 0x0b,
		GetSizeDL	= 0x0c,
		ItemSizeDL	= 0x0d,
		BaseSizeDL	= 0x0e,
		SetSizeDL	= 0x0f,

		DeleteItemDL	= 0x10,
		ShrinkDL	= 0x11,

		InsertDL	= 0x12,
		InsertListDL	= 0x13,

		PreapplyDL	= 0x14,

		CollectDL	= 0x15,
		GroupByKeyDL	= 0x16,
		CountByKeyDL	= 0x17,
		ExchangeDataByKeyDL = 0x18,
		GetKeyLocationDL	= 0x19

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
	using flatMapFunctionP = std::deque<U> (*) (T & input);
	template <typename T, typename L, typename U>
	using IflatMapFunctionP = std::deque<std::pair<L,U>> (*) (T & input);
	template <typename T, typename U>
	using PflatMapFunctionP = std::deque<std::pair<U,size_t>>  (*) (T & input);
	template <typename T, typename L, typename U>
	using IPflatMapFunctionP = std::deque<std::tuple<L, U,size_t>>  (*) (T & input);

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
	using flatMapPFunctionP = std::deque<U> (*) (T *& input, size_t size);
	template <typename T, typename L, typename U>
	using IflatMapPFunctionP = std::deque<std::pair<L,U>> (*) (T *& input, size_t size);
	template <typename T, typename U>
	using PflatMapPFunctionP = std::deque<std::pair<U, size_t>> (*) (T *& input, size_t size);
	template <typename T, typename L, typename U>
	using IPflatMapPFunctionP = std::deque<std::tuple<L, U, size_t>> (*) (T *& input, size_t size);


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

	// --------------- Indexed FDDs -------------- //
	// FDD function pointer types
	// TODO CHANGE THIS BELOW!
	template <typename K, typename T, typename L, typename U>
	using ImapIFunctionP = std::pair<L,U> (*) (const K & inKey, T & input);
	template <typename K, typename T, typename U>
	using mapIFunctionP = U (*) (const K & inKey, T & input);
	template <typename K, typename T, typename L, typename U>
	//using IPmapIFunctionP = void (*) (L & outKey, U & output, size_t &outputSize, T & input);
	using IPmapIFunctionP = std::tuple<L,U,size_t> (*) (const K & inKey, T & input);
	template <typename K, typename T, typename U>
	using PmapIFunctionP = std::pair<U, size_t> (*) (const K & inKey, T & input);

	/*
	template <typename K, typename T, typename L, typename U>
	using ImapByKeyIFunctionP = std::pair<L,U> (*) (const K & inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using mapByKeyIFunctionP = U (*) (const K & inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPmapByKeyIFunctionP = std::tuple<L,U,size_t> (*) (const K & inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using PmapByKeyIFunctionP = std::pair<U, size_t> (*) (const K & inKey, T * input, size_t size);
	*/
	template <typename K, typename T, typename L, typename U>
	using ImapByKeyIFunctionP = std::pair<L,U> (*) (const K & inKey, std::vector<T *> & input);
	template <typename K, typename T, typename U>
	using mapByKeyIFunctionP = U (*) (const K & inKey, std::vector <T *> & input);
	template <typename K, typename T, typename L, typename U>
	using IPmapByKeyIFunctionP = std::tuple<L,U,size_t> (*) (const K & inKey, std::vector <T *> & input);
	template <typename K, typename T, typename U>
	using PmapByKeyIFunctionP = std::pair<U, size_t> (*) (const K & inKey, std::vector <T *> & input);

	template <typename K, typename T, typename L, typename U>
	using IbulkMapIFunctionP = void (*) (L * outKey, U * output, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using bulkMapIFunctionP = void (*) (U * output, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkMapIFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using PbulkMapIFunctionP = void (*) (U * output, size_t * outputDataSizes, K * inKey, T * input, size_t size);

	template <typename K, typename T, typename L, typename U>
	using IflatMapIFunctionP = std::deque<std::pair<L,U>> (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using flatMapIFunctionP = std::deque<U> (*) (K inKey, T & input);
	template <typename K, typename T, typename L, typename U>
	using IPflatMapIFunctionP = std::deque<std::tuple<L, U,size_t>>  (*) (K inKey, T & input);
	template <typename K, typename T, typename U>
	using PflatMapIFunctionP = std::deque<std::pair<U, size_t>>  (*) (K inKey, T & input);

	template <typename K, typename T, typename L, typename U>
	using IbulkFlatMapIFunctionP = void (*) (L *& outKey, U *& output, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using bulkFlatMapIFunctionP = void (*) (U *& output, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkFlatMapIFunctionP = void (*) (L *& outKey, U *& output, size_t *& outputDataSizes, size_t & outputSize, K * inKey, T * input, size_t size);
	template <typename K, typename T, typename U>
	using PbulkFlatMapIFunctionP = void (*) (U *& output, size_t *& outputDataSizes, size_t & outputSize, K * inKey, T * input, size_t size);

	template <typename K, typename T>
	using IreduceIFunctionP = std::pair<K,T> (*) (const K & keyA, T & a, const K & keyB, T & b);

	template <typename K, typename T>
	using IreduceByKeyIFunctionP = std::pair<K,T> (*) (const K & keyA, T * a, size_t sizeA, const K & keyB, T * b, size_t sizeB);

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
	using ImapByKeyIPFunctionP = std::pair<L,U> (*) (const K & inKey, std::vector<std::pair<T*,size_t>>);
	template <typename K, typename T, typename U>
	using  mapByKeyIPFunctionP = U (*) (const K & inKey, std::vector<std::pair<T*,size_t>>);
	template <typename K, typename T, typename L, typename U>
	using IPmapByKeyIPFunctionP = std::tuple<L,U,size_t> (*) (const K & inKey, std::vector<std::pair<T*,size_t>>);
	template <typename K, typename T, typename U>
	using  PmapByKeyIPFunctionP = std::pair<U, size_t> (*) (const K & inKey, std::vector<std::pair<T*,size_t>>);

	template <typename K, typename T, typename L, typename U>
	using IbulkMapIPFunctionP = void (*) (L * outKey, U * output, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename U>
	using  bulkMapIPFunctionP = void (*) (U * output, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPbulkMapIPFunctionP = void (*) (L * outKey, U * output, size_t * outputDataSizes, K * inKey, T ** input, size_t * inputDataSizes, size_t size);
	template <typename K, typename T, typename U>
	using  PbulkMapIPFunctionP = void (*) (U * output, size_t * outputDataSizes, K * inKey, T ** input, size_t * inputDataSizes, size_t size);


	template <typename K, typename T, typename L, typename U>
	using IflatMapIPFunctionP = std::deque<std::pair<L,U>> (*) (T *& input, size_t size);
	template <typename K, typename T, typename U>
	using flatMapIPFunctionP = std::deque<U> (*) (T *& input, size_t size);
	template <typename K, typename T, typename L, typename U>
	using IPflatMapIPFunctionP = std::deque<std::tuple<L, U, size_t>> (*) (T *& input, size_t size);
	template <typename K, typename T, typename U>
	using PflatMapIPFunctionP = std::deque<std::pair<U, size_t>> (*) (T *& input, size_t size);


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
	using IPreduceByKeyIPFunctionP = std::tuple<K,T*,size_t> (*) (K keyA, T ** a, size_t * dataSizesA, size_t sizeA, K keyB, T ** b, size_t * dataSizesB, size_t sizeB);

	template <typename K, typename T>
	using IPbulkReduceIPFunctionP = std::tuple<K,T*,size_t> (*) (K * key, T ** input, size_t * inputDataSizes, size_t size);


	// --------------- Grouped FDDs -------------//

	template <typename K>
	using updateByKeyG2FunctionP = void (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b);

	template <typename K>
	using updateByKeyG3FunctionP = void (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b, std::vector<void*> & c);


	template <typename K>
	using bulkUpdateG2FunctionP = void (*) (K * keyA, void * a, size_t na, K * keyB, void * b, size_t nb);

	template <typename K>
	using bulkUpdateG3FunctionP = void (*) (K * keyA, void * a, size_t na, K * keyB, void * b, size_t nb, K * keyC, void * c, size_t nc);


	template <typename K, typename To>
	using mapByKeyG2FunctionP = To (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b);

	template <typename K, typename To>
	using mapByKeyG3FunctionP = To (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b, std::vector<void*> & c);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG2FunctionP = std::pair<Ko,To> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG3FunctionP = std::pair<Ko,To> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b, std::vector<void*> & c);


	template <typename K, typename To>
	using flatMapByKeyG2FunctionP = std::deque<To> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b);

	template <typename K, typename To>
	using flatMapByKeyG3FunctionP = std::deque<To> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b, std::vector<void*> & c);

	template <typename K, typename Ko, typename To>
	using IflatMapByKeyG2FunctionP = std::deque<std::pair<Ko,To>> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b);

	template <typename K, typename Ko, typename To>
	using IflatMapByKeyG3FunctionP = std::deque<std::pair<Ko,To>> (*) (const K & key, std::vector<void*> & a, std::vector<void*> & b, std::vector<void*> & c);


	template <typename K, typename To>
	using bulkFlatMapG2FunctionP = std::deque<To> (*) (K * keyA, void* a, size_t na, K * keyB, void* b, size_t nb);

	template <typename K, typename To>
	using bulkFlatMapG3FunctionP = std::deque<To> (*) (K * keyA, void* a, size_t na, K * keyB, void* b, size_t nb, K * keyC, void* c, size_t nc);

	template <typename K, typename Ko, typename To>
	using IbulkFlatMapG2FunctionP = std::deque<std::pair<Ko,To>> (*) (K * keyA, void* a, size_t na, K * keyB, void* b, size_t nb);

	template <typename K, typename Ko, typename To>
	using IbulkFlatMapG3FunctionP = std::deque<std::pair<Ko,To>> (*) (K * keyA, void* a, size_t na, K * keyB, void* b, size_t nb, K * keyC, void* c, size_t nc);


	/*template <typename K>
	using updateByKeyG2FunctionP = void (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K>
	using updateByKeyG3FunctionP = void (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);*/

	/*template <typename K, typename T, typename U, typename To>
	using mapByKeyG2FunctionP = To (*) (const K & key, T * a, size_t sizeA, U * b, size_t sizeB);

	template <typename K, typename T, typename U, typename V, typename To>
	using mapByKeyG3FunctionP = To (*) (const K & key, T * a, size_t sizeA, U * b, size_t sizeB, V * c, size_t sizeC);

	template <typename K, typename T, typename U, typename Ko, typename To>
	using ImapByKeyG2FunctionP = std::pair<Ko,To> (*) (const K & key, T * a, size_t sizeA, U * b, size_t sizeB);

	template <typename K, typename T, typename U, typename V, typename Ko, typename To>
	using ImapByKeyG3FunctionP = std::pair<Ko,To> (*) (const K & key, T * a, size_t sizeA, U * b, size_t sizeB, V * c, size_t sizeC);// */

	/*template <typename K, typename To>
	using mapByKeyG2FunctionP = To (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename To>
	using mapByKeyG3FunctionP = To (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG2FunctionP = std::pair<Ko,To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG3FunctionP = std::pair<Ko,To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);


	template <typename K, typename To>
	using flatMapByKeyG2FunctionP = std::deque<To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename To>
	using flatMapByKeyG3FunctionP = std::deque<To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);

	template <typename K, typename Ko, typename To>
	using IflatMapByKeyG2FunctionP = std::deque<std::pair<Ko,To>> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename Ko, typename To>
	using IflatMapByKeyG3FunctionP = std::deque<std::pair<Ko,To>> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);*/


	/*template <typename K, typename To>
	using mapByKeyG2VFunctionP = To (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename To>
	using mapByKeyG3VFunctionP = To (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG2VFunctionP = std::pair<Ko,To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB);

	template <typename K, typename Ko, typename To>
	using ImapByKeyG3VFunctionP = std::pair<Ko,To> (*) (const K & key, void * a, size_t sizeA, void * b, size_t sizeB, void * c, size_t sizeC); // */


	// DATA TYPES
	//template <typename K>
	//using CountKeyMapT = std::unordered_map<K, size_t> ;

	//template <typename K>
	//using PPCountKeyMapT = std::unordered_map<K, std::pair<size_t, std::deque<int>> > ;



}
#endif
