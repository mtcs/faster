#include <iostream>
#include <tuple>
#include "_workerFdd.h"
#include "workerFddBase.h"
#include "fddStorageExtern.cpp"
#include "fastComm.h"

#include "workerFddCoreExtern.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new _workerFdd<char *>(id, type, size); break;
		case IntP: return new _workerFdd<int *>(id, type, size); break;
		case LongIntP: return new _workerFdd<long int *>(id, type, size); break;
		case FloatP: return new _workerFdd<float *>(id, type, size); break;
		case DoubleP: return new _workerFdd<double *>(id, type, size); break;
		//case Custom: return new _workerFdd<void *>(id, type, size); break;
	}
	return NULL;
}

// MAP
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::map (workerFddBase * dest, mapPFunctionP<T,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		od[i] = mapFunc(d[i], ls[i]);
	}
	//std::cerr << "END ";
}

template <typename T>
template <typename U>
void faster::_workerFdd<T*>::map (workerFddBase * dest, PmapPFunctionP<T,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest->getLineSizes();
	U * od = (U*) dest->getData();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::pair<U,size_t> r = mapFunc(d[i], ls[i]);
		od[i] = r.first; 
		dls[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::map (workerFddBase * dest, ImapPFunctionP<T,L,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(d[i], ls[i]);
		ok[i] = r.first;
		od[i] = r.second;

	}
	//std::cerr << "END ";
}

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::map (workerFddBase * dest, IPmapPFunctionP<T,L,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest->getLineSizes();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::tuple<L,U,size_t> r = mapFunc(d[i], ls[i]);
		ok[i] = std::get<0>(r);
		od[i] = std::get<1>(r);
		dls[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		


// BulkMap
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::bulkMap (workerFddBase * dest, bulkMapPFunctionP<T,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((U*) dest->getData(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::bulkMap (workerFddBase * dest, PbulkMapPFunctionP<T,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((U*) dest->getData(), dest->getLineSizes(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::bulkMap (workerFddBase * dest, IbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((L*) dest->getKeys(), (U*) dest->getData(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::bulkMap (workerFddBase * dest, IPbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((L*) dest->getKeys(), (U*) dest->getData(), dest->getLineSizes(), (T **)this->localData->getData(), ls, s);
}


// FlatMap
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::flatMap(workerFddBase * dest,  flatMapPFunctionP<T,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<U> resultList;

	#pragma omp parallel
	{
		std::deque<U> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<U>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::flatMap(workerFddBase * dest,  PflatMapPFunctionP<T,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::pair<U, size_t>> resultList;

	#pragma omp parallel
	{
		std::deque<std::pair<U, size_t>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::pair<U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::flatMap(workerFddBase * dest,  IflatMapPFunctionP<T,L,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::pair<L,U>> resultList;

	#pragma omp parallel
	{
		std::deque<std::pair<L,U>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::pair<L,U>> r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::flatMap(workerFddBase * dest,  IPflatMapPFunctionP<T,L,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::tuple<L,U, size_t>> resultList;

	#pragma omp parallel
	{
		std::deque<std::tuple<L,U, size_t>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::tuple<L,U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}

template <typename T>
template <typename U>
void faster::_workerFdd<T*>::bulkFlatMap(workerFddBase * dest,  bulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc( result, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(result, rSize);
}
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::bulkFlatMap(workerFddBase * dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData((void*) result, rDataSizes, rSize);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::bulkFlatMap(workerFddBase * dest,  IbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(keys, result, rSize);
}
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rDataSizes, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(keys, (void*) result, rDataSizes, rSize);
}


// REDUCE
template <typename T>
std::pair<T*,size_t>  faster::_workerFdd<T*>::reduce (PreducePFunctionP<T> reduceFunc){
	T ** d = this->localData->getData();
	std::pair<T*,size_t> result;
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::pair<T *, size_t>  partResult(d[tN], ls[tN] );
		T * a, * b;
		size_t aSize, bSize;

		//#pragma omp master
		//std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			a = partResult.first;
			b = d[i];
			aSize = partResult.second;
			bSize = ls[i];

			partResult = reduceFunc(a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
		#pragma omp master
		{
			result = partResult;
		}

		#pragma omp barrier

		#pragma omp critical
		if (omp_get_thread_num() != 0){
			a = result.first;
			b = partResult.first;
			aSize = result.second;
			bSize = partResult.second;

			result = reduceFunc(a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
	}
	//std::cerr << "END ";
	return result;
}

template <typename T>
std::pair<T*,size_t> faster::_workerFdd<T*>::bulkReduce (PbulkReducePFunctionP<T> bulkReduceFunc){
	std::pair<T *, size_t> r = bulkReduceFunc((T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	return r;
}
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::_apply(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map(dest, (mapPFunctionP<T,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(dest, ( bulkMapPFunctionP<T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(dest, ( flatMapPFunctionP<T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(dest, ( bulkFlatMapPFunctionP<T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename T>
template <typename U>
void faster::_workerFdd<T*>::_applyP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map(dest, (PmapPFunctionP<T,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(dest, ( PbulkMapPFunctionP<T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(dest, ( PflatMapPFunctionP<T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(dest, ( PbulkFlatMapPFunctionP<T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::_applyI(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map(dest, (ImapPFunctionP<T,L,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(dest, ( IbulkMapPFunctionP<T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(dest, ( IflatMapPFunctionP<T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(dest, ( IbulkFlatMapPFunctionP<T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename T>
template <typename L, typename U>
void faster::_workerFdd<T*>::_applyIP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map(dest, (IPmapPFunctionP<T,L,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(dest, ( IPbulkMapPFunctionP<T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(dest, ( IPflatMapPFunctionP<T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(dest, ( IPbulkFlatMapPFunctionP<T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename T>
void faster::_workerFdd<T*>::_applyReduce(void * func, fddOpType op, fastCommBuffer & buffer){
	std::pair<T*,size_t> r;
	switch (op){
		case OP_Reduce:
			r = reduce(( PreducePFunctionP<T> ) func);
			//std::cerr << "Reduce " ;
			break;
		case OP_BulkReduce:
			r = bulkReduce(( PbulkReducePFunctionP<T> ) func);
			//std::cerr << "BulkReduce ";
			break;
	}
	buffer.write(r.first, r.second);

}


template <typename T>
void faster::_workerFdd<T*>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
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
void faster::_workerFdd<T*>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
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

template <typename T>
void faster::_workerFdd<T*>::apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){ 
	if (op & OP_GENERICREDUCE){
		_applyReduce(func, op, buffer);
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

template <typename T>
void faster::_workerFdd<T*>::setData(T ** data, size_t *lineSizes, size_t size) {
	this->localData->setData( data, lineSizes, size);
}

template <typename T>
void faster::_workerFdd<T*>::setDataRaw(void * data, size_t *lineSizes, size_t size) {
	this->localData->setDataRaw( data, lineSizes, size);
}


template <typename T>
size_t * faster::_workerFdd<T*>::getLineSizes(){ 
	return this->localData->getLineSizes(); 
}

template <typename T>
void faster::_workerFdd<T*>::insert(void * k UNUSED, void * in, size_t s){ 
	this->localData->insert((T*&)in, s);
}
template <typename T>
void faster::_workerFdd<T*>::insertl(void * in){ 
	insert( *(std::deque<std::pair<T*, size_t>>*) in);
}

template <typename T>
void faster::_workerFdd<T*>::insert(T* & in, size_t s){ 
	this->localData->insert(in, s); 
}
template <typename T>
void faster::_workerFdd<T*>::insert(std::deque< std::pair<T*, size_t> > & in){ 
	typename std::deque< std::pair<T*, size_t> >::iterator it;

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		this->localData->insert(it->first, it->second); 
}

template <typename T>
void faster::_workerFdd<T*>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());

};



template class faster::_workerFdd<char *>;
template class faster::_workerFdd<int *>;
template class faster::_workerFdd<long int *>;
template class faster::_workerFdd<float *>;
template class faster::_workerFdd<double *>;
//template class workerFdd<void *>;
 

