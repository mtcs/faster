#include <tuple>
#include <list>
#include <iostream>

#include "_workerIFdd.h"
#include "indexedFddStorageExtern.cpp"

// MAP
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::map (workerFddBase * dest, ImapIPFunctionP<K,T,L,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	K * ik = this->localData->getKeys();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(ik[i], d[i], ls[i]);
		ok[i] = r.first;
		od[i] = r.second;
	}
	//std::cerr << "END ";
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::map (workerFddBase * dest, IPmapIPFunctionP<K,T,L,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest->getLineSizes();
	K * ik = this->localData->getKeys();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::tuple<L,U,size_t> r = mapFunc(ik[i], d[i], ls[i]);
		ok[i] = std::get<0>(r);
		od[i] = std::get<1>(r);
		dls[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::map (workerFddBase * dest, mapIPFunctionP<K,T,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = this->localData->getKeys();
	size_t * ls = this->localData->getLineSizes();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		od[i] = mapFunc(ik[i], d[i], ls[i]);
	}
	//std::cerr << "END ";
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::map (workerFddBase * dest, PmapIPFunctionP<K,T,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = this->localData->getKeys();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest->getLineSizes();
	U * od = (U*) dest->getData();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::pair<U,size_t> r = mapFunc(ik[i], d[i], ls[i]);
		od[i] = r.first;
		dls[i] = r.second;
	}
	//std::cerr << "END ";
}		



// Bulk MAP

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::bulkMap (workerFddBase * dest, IbulkMapIPFunctionP<K,T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	K * ik = this->localData->getKeys();
	L * ok = (L*) dest->getKeys();

	bulkMapFunc(ok, (U*) dest->getData(), ik, (T **)this->localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::bulkMap (workerFddBase * dest, IPbulkMapIPFunctionP<K,T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	K * ik = this->localData->getKeys();
	L * ok = (L*) dest->getKeys();

	bulkMapFunc(ok, (U*) dest->getData(), dest->getLineSizes(), ik, (T **)this->localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::bulkMap (workerFddBase * dest, bulkMapIPFunctionP<K,T,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	K * ik = this->localData->getKeys();

	bulkMapFunc((U*) dest->getData(), ik, (T **)this->localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::bulkMap (workerFddBase * dest, PbulkMapIPFunctionP<K,T,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	K * ik = this->localData->getKeys();

	bulkMapFunc((U*) dest->getData(), dest->getLineSizes(), ik, (T **)this->localData->getData(), ls, s);
}


template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::flatMap(workerFddBase * dest,  IflatMapIPFunctionP<K,T,L,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::pair<L,U>> resultList;

	#pragma omp parallel
	{
		std::deque<std::pair<L,U>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::pair<L,U>> r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::flatMap(workerFddBase * dest,  IPflatMapIPFunctionP<K,T,L,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::tuple<L, U, size_t>> resultList;

	#pragma omp parallel
	{
		std::deque<std::tuple<L, U, size_t>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::tuple<L, U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::flatMap(workerFddBase * dest,  flatMapIPFunctionP<K,T,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<U> resultList;

	#pragma omp parallel
	{
		std::deque<U> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<U> r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::flatMap(workerFddBase * dest,  PflatMapIPFunctionP<K,T,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::deque<std::pair<U, size_t>> resultList;

	#pragma omp parallel
	{
		std::deque<std::pair<U, size_t>> partResultList;
		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::deque<std::pair<U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest->insertl(&resultList);
}




template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::bulkFlatMap(workerFddBase * dest,  IbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t rSize;
	K * ik = this->localData->getKeys();

	bulkFlatMapFunc( ok, result, rSize, ik, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(ok, result, rSize);
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;
	K * ik = this->localData->getKeys();

	bulkFlatMapFunc( ok, result, rDataSizes, rSize, ik, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(ok, result, rDataSizes, rSize);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::bulkFlatMap(workerFddBase * dest,  bulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;
	K * ik = this->localData->getKeys();

	bulkFlatMapFunc( result, rSize, ik, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(result, rSize);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::bulkFlatMap(workerFddBase * dest,  PbulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;
	K * ik = this->localData->getKeys();

	bulkFlatMapFunc( result, rDataSizes, rSize, ik, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest->setData(result, rDataSizes, rSize);
}




// Pointer -> Not Pointer
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::_applyI(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<L,U>(dest, (ImapIPFunctionP<K,T,L,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap<L,U>(dest, ( IbulkMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<L,U>(dest, ( IflatMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<L,U>(dest, ( IbulkFlatMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T*>::_applyIP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<L,U>(dest, (IPmapIPFunctionP<K,T,L,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap<L,U>(dest, ( IPbulkMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<L,U>(dest, ( IPflatMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<L,U>(dest, ( IPbulkFlatMapIPFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::_apply(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<U>(dest, (mapIPFunctionP<K,T,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap<U>(dest, ( bulkMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<U>(dest, ( flatMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<U>(dest, ( bulkFlatMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T*>::_applyP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<U>(dest, (PmapIPFunctionP<K,T,U>) func);
			//std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap<U>(dest, ( PbulkMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<U>(dest, ( PflatMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<U>(dest, ( PbulkFlatMapIPFunctionP<K,T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}


template <typename K, typename T>
template <typename L>
void faster::_workerIFdd<K,T*>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: 	break;
		case Char:      _applyI<L,char>	(func, op, dest); break;
		case Int:       _applyI<L,int> 	(func, op,  dest); break;
		case LongInt:   _applyI<L,long int>(func, op,  dest); break;
		case Float:     _applyI<L,float>   (func, op,  dest); break;
		case Double:    _applyI<L,double>  (func, op,  dest); break;
		case CharP:    _applyIP<L,char *>  (func, op, dest); break;
		case IntP:     _applyIP<L,int *>   (func, op, dest); break;
		case LongIntP: _applyIP<L,long int *> (func, op, dest); break;
		case FloatP:   _applyIP<L,float *>    (func, op, dest); break;
		case DoubleP:  _applyIP<L,double *>   (func, op, dest); break;
		case String:   _applyI<L,std::string>(func, op,  dest); break;
		//case Custom:  _applyI(func, op, dest); break;
		case CharV:    _applyI<L,std::vector<char>>    (func, op, dest); break;
		case IntV:     _applyI<L,std::vector<int>>     (func, op, dest); break;
		case LongIntV: _applyI<L,std::vector<long int>>(func, op, dest); break;
		case FloatV:   _applyI<L,std::vector<float>>   (func, op, dest); break;
		case DoubleV:  _applyI<L,std::vector<double>>  (func, op, dest); break;

	}

}

template <typename K, typename T>
void faster::_workerIFdd<K,T*>::_preApply(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: 	break;

		case Char:      _apply<char> 	(func, op, dest); break;
		case Int:       _apply<int> 	(func, op, dest); break;
		case LongInt:   _apply<long int> (func, op, dest); break;
		case Float:     _apply<float> 	(func, op, dest); break;
		case Double:    _apply<double> 	(func, op, dest); break;
		case CharP:    _applyP<char *> 	(func, op, dest); break;
		case IntP:     _applyP<int *> 	(func, op, dest); break;
		case LongIntP: _applyP<long int *> (func, op, dest); break;
		case FloatP:   _applyP<float *> (func, op, dest); break;
		case DoubleP:  _applyP<double *> (func, op, dest); break;
		case String:    _apply<std::string>(func, op, dest); break;
		//case Custom:  _apply<void *>(func, op, dest); break;
		case CharV:   _apply<std::vector<char>>  (func, op,  dest); break;
		case IntV:    _apply<std::vector<int>> 	 (func, op,  dest); break;
		case LongIntV:_apply<std::vector<long int>>(func, op,  dest); break;
		case FloatV:  _apply<std::vector<float>>  (func, op,  dest); break;
		case DoubleV: _apply<std::vector<double>> (func, op,  dest); break;
	}

}



template <typename K, typename T>
void faster::_workerIFdd<K,T*>::applyDependent(void * func, fddOpType op, workerFddBase * dest){ 
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


