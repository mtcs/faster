#include <iostream>
#include <tuple>
#include "_workerFdd.h"
#include "workerFddBase.h"
#include "fddStorageExtern.cpp"
#include "fastComm.h"

#include "workerFddCoreExtern.cpp"

// MAP
template <typename T>
template <typename U>
void faster::_workerFdd<T>::map (workerFddBase * dest, mapFunctionP<T,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		od[i] = mapFunc(d[i]);
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename U>
void faster::_workerFdd<T>::map (workerFddBase * dest, PmapFunctionP<T,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = dest->getLineSizes() ;
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<U,size_t> r = mapFunc(d[i]);
		od[i] = r.first;
		ls[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::map (workerFddBase * dest, ImapFunctionP<T,L,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << s << " \n ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		//std::cerr << i << " ";
		std::pair<L,U> r = mapFunc(d[i]);
		ok[i] = r.first;
		od[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::map (workerFddBase * dest, IPmapFunctionP<T,L,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = dest->getLineSizes() ;
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::tie(ok[i], od[i], ls[i]) = mapFunc(d[i]);
	}
	//std::cerr << "END ";
}		


// BulkMap
template <typename T>
template <typename U>
void faster::_workerFdd<T>::bulkMap (workerFddBase * dest, bulkMapFunctionP<T,U> bulkMapFunc){
	bulkMapFunc((U*) dest->getData(), (T *)this->localData->getData(), this->localData->getSize());
}
template <typename T>
template <typename U>
void faster::_workerFdd<T>::bulkMap (workerFddBase * dest, PbulkMapFunctionP<T,U> bulkMapFunc){
	bulkMapFunc((U*) dest->getData(), dest->getLineSizes(), (T *) this->localData->getData(), this->localData->getSize());
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::bulkMap (workerFddBase * dest, IbulkMapFunctionP<T,L,U> bulkMapFunc){
	bulkMapFunc((L*) dest->getKeys(), (U*) dest->getData(), (T *)this->localData->getData(), this->localData->getSize());
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::bulkMap (workerFddBase * dest, IPbulkMapFunctionP<T,L,U> bulkMapFunc){
	bulkMapFunc((L*) dest->getKeys(), (U*) dest->getData(), dest->getLineSizes(), (T *) this->localData->getData(), this->localData->getSize());
}


// FlatMap
template <typename T>
template <typename U>
void faster::_workerFdd<T>::flatMap(workerFddBase * dest,  flatMapFunctionP<T,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
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
	dest->insertl(&resultList);
}
template <typename T>
template <typename U>
void faster::_workerFdd<T>::flatMap(workerFddBase * dest,  PflatMapFunctionP<T,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
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
	dest->insertl(&resultList);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::flatMap(workerFddBase * dest,  IflatMapFunctionP<T,L,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
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
	dest->insertl(&resultList);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::flatMap(workerFddBase * dest,  IPflatMapFunctionP<T,L,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
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
	dest->insertl(&resultList);
}

// BlockFlatMap
template <typename T>
template <typename U>
void faster::_workerFdd<T>::bulkFlatMap(workerFddBase * dest,  bulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc(result, rSize, this->localData->getData(), this->localData->getSize());
	dest->setData(result, rSize);
}
template <typename T>
template <typename U>
void faster::_workerFdd<T>::bulkFlatMap(workerFddBase * dest,  PbulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, (T*) this->localData->getData(), this->localData->getSize());
	dest->setData( result, rDataSizes, rSize);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::bulkFlatMap(workerFddBase * dest,  IbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t rSize;

	bulkFlatMapFunc(keys, result, rSize, this->localData->getData(), this->localData->getSize());
	dest->setData(keys, result, rSize);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc(keys, result, rDataSizes, rSize, (T*) this->localData->getData(), this->localData->getSize());
	dest->setData(keys, result, rDataSizes, rSize);
}


// REDUCE
template <typename T>
T faster::_workerFdd<T>::reduce (reduceFunctionP<T> reduceFunc){
	T * d = this->localData->getData();
	T result;
	size_t s = this->localData->getSize();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		T partResult = d[tN];

		//#pragma omp master
		//std::cerr << "Thread:"<< tN << "(" << nT << ")" << " ";
		
		#pragma omp for 
		for (int i = nT; i < s; ++i){
			partResult = reduceFunc(partResult, d[i]);
			//std::cerr << partResult<< "<" << d[i] << "  " ;
		}
		#pragma omp master
		result = partResult;

		#pragma omp barrier
		
		#pragma omp critical
		if (omp_get_thread_num() != 0){
			result = reduceFunc(result, partResult);
		}
	}
	//std::cerr << "END";
	//std::cerr << "END (RESULT:" << result << ")";
	return result;
}

template <typename T>
T faster::_workerFdd<T>::bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc){
	return bulkReduceFunc((T*) this->localData->getData(), this->localData->getSize());
}



template <typename T>
template <typename U>
void faster::_workerFdd<T>::_apply(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			std::cerr << "Map ";
			map<U>(dest, (mapFunctionP<T,U>) func);
			break;
		case OP_BulkMap:
			std::cerr << "BulkMap ";
			bulkMap<U>(dest, ( bulkMapFunctionP<T,U> ) func);
			break;
		case OP_FlatMap:
			std::cerr << "FlatMap ";
			flatMap<U>(dest, ( flatMapFunctionP<T,U> ) func);
			break;
		case OP_BulkFlatMap:
			std::cerr << "BulkFlatMap ";
			bulkFlatMap<U>(dest, ( bulkFlatMapFunctionP<T,U> ) func);
			break;
	}
}

// Not Pointer -> Pointer
template <typename T>
template <typename U>
void faster::_workerFdd<T>::_applyP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			std::cerr << "Map ";
			map(dest, (PmapFunctionP<T,U>) func);
			break;
		case OP_BulkMap:
			std::cerr << "BulkMap ";
			bulkMap(dest, ( PbulkMapFunctionP<T,U> ) func);
			break;
		case OP_FlatMap:
			std::cerr << "FlatMap ";
			flatMap(dest, ( PflatMapFunctionP<T,U> ) func);
			break;
		case OP_BulkFlatMap:
			std::cerr << "BulkFlatMap ";
			bulkFlatMap(dest, ( PbulkFlatMapFunctionP<T,U> ) func);
			break;
	}
}

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::_applyI(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			std::cerr << "Map ";
			map(dest, ( ImapFunctionP<T,L,U> ) func);
			break;
		case OP_BulkMap:
			std::cerr << "BulkMap ";
			bulkMap(dest, ( IbulkMapFunctionP<T,L,U> ) func);
			break;
		case OP_FlatMap:
			std::cerr << "FlatMap ";
			flatMap(dest, ( IflatMapFunctionP<T,L,U> ) func);
			break;
		case OP_BulkFlatMap:
			std::cerr << "BulkFlatMap ";
			bulkFlatMap(dest, ( IbulkFlatMapFunctionP<T,L,U> ) func);
			break;
	}
}

// Not Pointer -> Pointer
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T>::_applyIP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			std::cerr << "Map ";
			map(dest, ( IPmapFunctionP<T,L,U> ) func);
			break;
		case OP_BulkMap:
			std::cerr << "BulkMap ";
			bulkMap(dest, ( IPbulkMapFunctionP<T,L,U> ) func);
			break;
		case OP_FlatMap:
			std::cerr << "FlatMap ";
			flatMap(dest, ( IPflatMapFunctionP<T,L,U> ) func);
			break;
		case OP_BulkFlatMap:
			std::cerr << "BulkFlatMap ";
			bulkFlatMap(dest, ( IPbulkFlatMapFunctionP<T,L,U> ) func);
			break;
	}
}

template <typename T>
void faster::_workerFdd<T>::_applyReduce(void * func, fddOpType op, fastCommBuffer & buffer){
	T r;
	switch (op){
		case OP_Reduce:
			std::cerr << "Reduce ";
			r = reduce( ( reduceFunctionP<T> ) func);
			break;
		case OP_BulkReduce:
			std::cerr << "BulkReduce ";
			r = bulkReduce( ( bulkReduceFunctionP<T> ) func);
			break;
	}
	buffer << r;
}


template <typename T>
void faster::_workerFdd<T>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Char:      _apply<char>(func, op,  dest); break;
		case Int:       _apply<int>(func, op,  dest); break;
		case LongInt:   _apply<long int>(func, op, dest); break;
		case Float:     _apply<float>(func, op,  dest); break;
		case Double:    _apply<double>(func, op,  dest); break;
		case CharP:    _applyP<char *>(func, op, dest); break;
		case IntP:     _applyP<int *>(func, op, dest); break;
		case LongIntP: _applyP<long int *>(func, op, dest); break;
		case FloatP:   _applyP<float *>(func, op, dest); break;
		case DoubleP:  _applyP<double *>(func, op, dest); break;
		case String:    _apply<std::string>(func, op,  dest); break;
		//case Custom:   _apply<void *>(func, op, (workerFdd *) dest); break;
		case CharV:     _apply<std::vector<char>>(func, op, dest); break;
		case IntV:      _apply<std::vector<int>>(func, op, dest); break;
		case LongIntV:  _apply<std::vector<long int>>(func, op, dest); break;
		case FloatV:    _apply<std::vector<float>>(func, op, dest); break;
		case DoubleV:   _apply<std::vector<double>>(func, op, dest); break;
	}
}
template <typename T>
template <typename L>
void faster::_workerFdd<T>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:      _applyI<L, char> 	(func, op, dest); break;
		case Int:       _applyI<L, int> 	(func, op, dest); break;
		case LongInt:   _applyI<L, long int> 	(func, op, dest); break;
		case Float:     _applyI<L, float> 	(func, op, dest); break;
		case Double:    _applyI<L, double> 	(func, op, dest); break;
		case CharP:    _applyIP<L, char *> 	(func, op, dest); break;
		case IntP:     _applyIP<L, int *> 	(func, op, dest); break;
		case LongIntP: _applyIP<L, long int *> 	(func, op, dest); break;
		case FloatP:   _applyIP<L, float *> 	(func, op, dest); break;
		case DoubleP:  _applyIP<L, double *> 	(func, op, dest); break;
		case String:    _applyI<L, std::string> (func, op, dest); break;
		//case Custom:    _applyI<L, void *> 	(func, op, dest); break;
		case CharV:     _applyI<L, std::vector<char>> 	(func, op, dest); break;
		case IntV:      _applyI<L, std::vector<int>> 	(func, op, dest); break;
		case LongIntV:  _applyI<L, std::vector<long int>>(func, op, dest); break;
		case FloatV:    _applyI<L, std::vector<float>> 	(func, op, dest); break;
		case DoubleV:   _applyI<L, std::vector<double>> (func, op, dest); break;
	}
}


// -------------------------- Public Functions ------------------------ //



// For known primitive types
template <typename T>
void faster::_workerFdd<T>::setData(T * data, size_t size) {
	this->localData->setData(data, size);
}
// For anonymous primitive types
template <typename T>
void faster::_workerFdd<T>::setDataRaw(void * data, size_t size) {
	this->localData->setDataRaw(data, size);
}

template <typename T>
void faster::_workerFdd<T>::insert(void * k UNUSED, void * in, size_t s UNUSED){ 
	this->localData->insert(* (T*) in);
}
template <typename T>
void faster::_workerFdd<T>::insertl(void * in){ 
	insert(* (std::list<T> *) in);
}


template <typename T>
void faster::_workerFdd<T>::insert(T & in){ 
	this->localData->insert(in); 
}
// TODO CHANGE THIS !!!! MAK THE LIST ITERATION INSIDE THE STORAGE
template <typename T>
void faster::_workerFdd<T>::insert(std::list<T> & in){ 
	typename std::list<T>::iterator it;

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		this->localData->insert(*it); 
}


template <typename T>
void faster::_workerFdd<T>::apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){ 
	if (op & OP_GENERICMAP){
		switch (dest->getKeyType()){
			case Null:     _preApply(func, op, dest);break;
			case Char:     _preApplyI<char>(func, op, dest); break;
			case Int:      _preApplyI<int>(func, op, dest); break;
			case LongInt:  _preApplyI<long int>(func, op, dest); break;
			case Float:    _preApplyI<float>(func, op, dest); break;
			case Double:   _preApplyI<double>(func, op, dest); break;
			case String:   _preApplyI<std::string>(func, op, dest); break;
		}
	}else{
		_applyReduce(func, op, buffer);
	}
}


template <typename T>
void faster::_workerFdd<T>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getData(), this->localData->getSize());
};
