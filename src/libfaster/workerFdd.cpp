#include <iostream>
#include <tuple>
#include "fddStorage.h"
#include "workerFdd.h"
#include "workerIFdd.h"
#include "fastCommBuffer.h"

workerFddBase::workerFddBase() {
	resultBuffer = new fastCommBuffer();
}
workerFddBase::workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {
	resultBuffer = new fastCommBuffer();
}

workerFddBase::~workerFddBase() {
	delete resultBuffer;
};

// MAP
template <typename T>
template <typename U>
void workerFdd<T>::map (workerFdd<U> & dest, mapFunctionP<T,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		dest[i] = mapFunc(d[i]);
		//std::cerr << dest[i];
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename U>
void workerFdd<T>::map (workerFdd<U> & dest, PmapFunctionP<T,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = dest.getLineSizes() ;

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<U,size_t> r = mapFunc(d[i]);
		dest[i] = r.first;
		ls[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void workerFdd<T>::map (workerIFdd<L,U> & dest, ImapFunctionP<T,L,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(d[i]);
		ok[i] = r.first;
		dest[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void workerFdd<T>::map (workerIFdd<L,U> & dest, IPmapFunctionP<T,L,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = dest.getLineSizes() ;
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::tuple<L,U,size_t> r = mapFunc(d[i]);
		std::tie(ok[i], dest[i], ls[i]) = r;
	}
	//std::cerr << "END ";
}		


// BulkMap
template <typename T>
template <typename U>
void workerFdd<T>::bulkMap (workerFdd<U> & dest, bulkMapFunctionP<T,U> bulkMapFunc){
	bulkMapFunc((U*) dest.getData(), (T *)localData->getData(), localData->getSize());
}
template <typename T>
template <typename U>
void workerFdd<T>::bulkMap (workerFdd<U> & dest, PbulkMapFunctionP<T,U> bulkMapFunc){
	bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (T *) localData->getData(), localData->getSize());
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::bulkMap (workerIFdd<L,U> & dest, IbulkMapFunctionP<T,L,U> bulkMapFunc){
	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), (T *)localData->getData(), localData->getSize());
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::bulkMap (workerIFdd<L,U> & dest, IPbulkMapFunctionP<T,L,U> bulkMapFunc){
	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), dest.getLineSizes(), (T *) localData->getData(), localData->getSize());
}


// FlatMap
template <typename T>
template <typename U>
void workerFdd<T>::flatMap(workerFdd<U> & dest,  flatMapFunctionP<T,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	std::list<U> resultList;

	#pragma omp parallel 
	{
		std::list<U> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<U> r = flatMapFunc(d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename T>
template <typename U>
void workerFdd<T>::flatMap(workerFdd<U> & dest,  PflatMapFunctionP<T,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	std::list< std::pair<U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>> r = flatMapFunc(d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::flatMap(workerIFdd<L,U> & dest,  IflatMapFunctionP<T,L,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<L,U>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::flatMap(workerIFdd<L,U> & dest,  IPflatMapFunctionP<T,L,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	std::list< std::tuple<L, U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::tuple<L, U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::tuple<L, U, size_t>> r = flatMapFunc(d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}

// BlockFlatMap
template <typename T>
template <typename U>
void workerFdd<T>::bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc(result, rSize, localData->getData(), localData->getSize());
	dest.setData(result, rSize);
}
template <typename T>
template <typename U>
void workerFdd<T>::bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, (T*) localData->getData(), localData->getSize());
	dest.setData( (void**) result, rDataSizes, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t rSize;

	bulkFlatMapFunc(keys, result, rSize, localData->getData(), localData->getSize());
	dest.setData(keys, result, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T>::bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc(keys, result, rDataSizes, rSize, (T*) localData->getData(), localData->getSize());
	dest.setData(keys, (void**) result, rDataSizes, rSize);
}


// REDUCE
template <typename T>
T workerFdd<T>::reduce (reduceFunctionP<T> reduceFunc){
	T * d = localData->getData();
	T result;
	size_t s = localData->getSize();
	std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		T partResult = d[tN];

		#pragma omp master
		std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			partResult = reduceFunc(partResult, d[i]);
		}
		#pragma omp master
		result = partResult;
		
		#pragma omp barrier
		
		#pragma omp critical
		if (omp_get_thread_num() != 0){
			result = reduceFunc(result, partResult);
		}
	}
	std::cerr << "END (RESULT:"<< result << ")";
	return result;
}

template <typename T>
T workerFdd<T>::bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc){
	return bulkReduceFunc((T*) localData->getData(), localData->getSize());
}



template <typename T>
template <typename U>
void workerFdd<T>::_apply(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (mapFunctionP<T,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( bulkMapFunctionP<T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( flatMapFunctionP<T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( bulkFlatMapFunctionP<T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <typename T>
template <typename U>
void workerFdd<T>::_applyP(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (PmapFunctionP<T,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( PbulkMapFunctionP<T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( PflatMapFunctionP<T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( PbulkFlatMapFunctionP<T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename T>
template <typename L, typename U>
void workerFdd<T>::_applyI(void * func, fddOpType op, workerIFdd<L,U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, ( ImapFunctionP<T,L,U> ) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IbulkMapFunctionP<T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IflatMapFunctionP<T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IbulkFlatMapFunctionP<T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <typename T>
template <typename L, typename U>
void workerFdd<T>::_applyIP(void * func, fddOpType op, workerIFdd<L,U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, ( IPmapFunctionP<T,L,U> ) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IPbulkMapFunctionP<T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IPflatMapFunctionP<T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IPbulkFlatMapFunctionP<T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename T>
void workerFdd<T>::_applyReduce(void * func, fddOpType op, void * result, size_t & rSize){
	T r;
	switch (op){
		case OP_Reduce:
			r = reduce( ( reduceFunctionP<T> ) func);
			std::cerr << "Reduce ";
			break;
		case OP_BulkReduce:
			r = bulkReduce( ( bulkReduceFunctionP<T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}
	resultBuffer->reset();
	(*resultBuffer) << r;
	result = resultBuffer->data();
	rSize = resultBuffer->size();
}


template <typename T>
void workerFdd<T>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _apply(func, op,  (workerFdd<char> *) dest); break;
		case Int:      _apply(func, op,  (workerFdd<int> *) dest); break;
		case LongInt:  _apply(func, op,  (workerFdd<long int> *) dest); break;
		case Float:    _apply(func, op,  (workerFdd<float> *) dest); break;
		case Double:   _apply(func, op,  (workerFdd<double> *) dest); break;
		case CharP:    _applyP(func, op, (workerFdd<char *> *) dest); break;
		case IntP:     _applyP(func, op, (workerFdd<int *> *) dest); break;
		case LongIntP: _applyP(func, op, (workerFdd<long int *> *) dest); break;
		case FloatP:   _applyP(func, op, (workerFdd<float *> *) dest); break;
		case DoubleP:  _applyP(func, op, (workerFdd<double *> *) dest); break;
		case String:   _apply(func, op,  (workerFdd<std::string> *) dest); break;
		//case Custom:   _apply(func, op, (workerFdd<void *> *) dest); break;
		//case CharV:     _apply(func, op, (workerFdd<std::vector<char>> *) dest); break;
		//case IntV:      _apply(func, op, (workerFdd<std::vector<int>> *) dest); break;
		//case LongIntV:  _apply(func, op, (workerFdd<std::vector<long int>> *) dest); break;
		//case FloatV:    _apply(func, op, (workerFdd<std::vector<float>> *) dest); break;
		//case DoubleV:   _apply(func, op, (workerFdd<std::vector<double>> *) dest); break;
	}
}
template <typename T>
template <typename L>
void workerFdd<T>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyI(func, op,  (workerIFdd<L, char> *) dest); break;
		case Int:      _applyI(func, op,  (workerIFdd<L, int> *) dest); break;
		case LongInt:  _applyI(func, op,  (workerIFdd<L, long int> *) dest); break;
		case Float:    _applyI(func, op,  (workerIFdd<L, float> *) dest); break;
		case Double:   _applyI(func, op,  (workerIFdd<L, double> *) dest); break;
		case CharP:    _applyIP(func, op, (workerIFdd<L, char *> *) dest); break;
		case IntP:     _applyIP(func, op, (workerIFdd<L, int *> *) dest); break;
		case LongIntP: _applyIP(func, op, (workerIFdd<L, long int *> *) dest); break;
		case FloatP:   _applyIP(func, op, (workerIFdd<L, float *> *) dest); break;
		case DoubleP:  _applyIP(func, op, (workerIFdd<L, double *> *) dest); break;
		case String:   _applyI(func, op,  (workerIFdd<L, std::string> *) dest); break;
		//case Custom:   _applyI(func, op, (workerFdd<L, void *> *) dest); break;
		//case CharV:     _applyI(func, op, (workerFdd<L, std::vector<char>> *) dest); break;
		//case IntV:      _applyI(func, op, (workerFdd<L, std::vector<int>> *) dest); break;
		//case LongIntV:  _applyI(func, op, (workerFdd<L, std::vector<long int>> *) dest); break;
		//case FloatV:    _applyI(func, op, (workerFdd<L, std::vector<float>> *) dest); break;
		//case DoubleV:   _applyI(func, op, (workerFdd<L, std::vector<double>> *) dest); break;
	}
}

template <typename T>
void workerFdd<T>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	if (op & OP_GENERICREDUCE){
		_applyReduce(func, op, result, rSize);
	}else{
		switch (dest->getKeyType()){
			case Null:     _preApply(func, op, dest);break;
			case Char:     _preApplyI<char>(func, op, dest); break;
			case Int:      _preApplyI<int>(func, op, dest); break;
			case LongInt:  _preApplyI<long int>(func, op, dest); break;
			case Float:    _preApplyI<float>(func, op, dest); break;
			case Double:   _preApplyI<double>(func, op, dest); break;
			case String:   _preApplyI<std::string>(func, op, dest); break;
		}
	}
}

template class workerFdd<char>;
template class workerFdd<int>;
template class workerFdd<long int>;
template class workerFdd<float>;
template class workerFdd<double>;
template class workerFdd<std::string>;
//template class workerFdd<std::vector<char>>;
//template class workerFdd<std::vector<int>>;
//template class workerFdd<std::vector<long int>>;
//template class workerFdd<std::vector<float>>;
//template class workerFdd<std::vector<double>>;

