#include <list>
#include <iostream>

#include "workerIFdd.h"
#include "workerFdd.h"
#include "indexedFddStorage.h"


// --------- MAP
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::map (workerIFdd<L,U> & dest, ImapIFunctionP<K,T,L,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(ik[i], d[i]);
		ok[i] = r.first;
		dest[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::map (workerIFdd<L,U> & dest, IPmapIFunctionP<K,T,L,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	size_t * ols = dest.getLineSizes() ;
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		//mapFunc(ok[i], dest[i], ols[i], d[i]);
		std::tuple <L,U,size_t> r = mapFunc(ik[i],  d[i]);
		ok[i] = std::get<0>(r);
		dest[i] = std::get<1>(r);
		ols[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template < typename U>
void workerIFdd<K,T>::map (workerFdd<U> & dest, mapIFunctionP<K,T,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		dest[i] = mapFunc(ik[i], d[i]);
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::map (workerFdd<U> & dest, PmapIFunctionP<K,T,U> mapFunc){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	size_t * ols = dest.getLineSizes() ;

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		//mapFunc(dest[i], ols[i], d[i]);
		std::pair<U, size_t> r = mapFunc(ik[i], d[i]);
		dest[i] = r.first;
		ols[i] = r.second;
	}
	//std::cerr << "END ";
}		



// --------- BulkMAP
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::bulkMap (workerIFdd<L,U> & dest, IbulkMapIFunctionP<K,T,L,U> bulkMapFunc){
	bulkMapFunc((L*) (dest.getKeys()), (U *) dest.getData(), (K*) localData->getKeys(), (T *)localData->getData(), localData->getSize());
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::bulkMap (workerIFdd<L,U> & dest, IPbulkMapIFunctionP<K,T,L,U> bulkMapFunc){
	bulkMapFunc((L*) (dest.getKeys()), (U*) dest.getData(), dest.getLineSizes(), (K*) localData->getKeys(), (T *) localData->getData(), localData->getSize());
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkMap (workerFdd<U> & dest, bulkMapIFunctionP<K,T,U> bulkMapFunc){
	bulkMapFunc((U*) dest.getData(), (K*) localData->getKeys(), (T *)localData->getData(), localData->getSize());
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkMap (workerFdd<U> & dest, PbulkMapIFunctionP<K,T,U> bulkMapFunc){
	bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (K*) localData->getKeys(), (T *) localData->getData(), localData->getSize());
}


// --------- FlatMAP

template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::flatMap(workerIFdd<L,U> & dest,  IflatMapIFunctionP<K,T,L,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<L,U>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::flatMap(workerIFdd<L,U> & dest,  IPflatMapIFunctionP<K,T,L,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	std::list< std::tuple<L, U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::tuple<L, U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::tuple<L, U, size_t>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::flatMap(workerFdd<U> & dest,  flatMapIFunctionP<K,T,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	std::list<U> resultList;

	#pragma omp parallel 
	{
		std::list<U> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<U> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::flatMap(workerFdd<U> & dest,  PflatMapIFunctionP<K,T,U> flatMapFunc ){
	T * d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	std::list< std::pair<U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest.insert(resultList);
}

template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t rSize;

	bulkFlatMapFunc(ok, result, rSize, localData->getKeys(), localData->getData(), localData->getSize());
	dest.setData(ok, result, rSize);
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc(ok, result, rDataSizes, rSize, localData->getKeys(), (T*) localData->getData(), localData->getSize());
	dest.setData( ok, (void**) result, rDataSizes, rSize);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc(result, rSize, localData->getKeys(), localData->getData(), localData->getSize());
	dest.setData(result, rSize);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, localData->getKeys(), (T*) localData->getData(), localData->getSize());
	dest.setData( result, rDataSizes, rSize);
}



// Not Pointer -> Not Pointer
template <class K, class T>
template <typename L, typename U>
void workerIFdd<K,T>::_applyI(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (ImapIFunctionP<K,T,L,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IbulkMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IflatMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IbulkFlatMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <class K, class T>
template <typename L, typename U>
void workerIFdd<K,T>::_applyIP(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (IPmapIFunctionP<K,T,L,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IPbulkMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IPflatMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IPbulkFlatMapIFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}
// Not Pointer -> Not Pointer
template <class K, class T>
template <typename U>
void workerIFdd<K,T>::_apply(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (mapIFunctionP<K,T,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( bulkMapIFunctionP<K,T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( flatMapIFunctionP<K,T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( bulkFlatMapIFunctionP<K,T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <class K, class T>
template <typename U>
void workerIFdd<K,T>::_applyP(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (PmapIFunctionP<K,T,U>) func);
			std::cerr << "Map ";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( PbulkMapIFunctionP<K,T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( PflatMapIFunctionP<K,T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( PbulkFlatMapIFunctionP<K,T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}


template <class K, class T>
template <typename L>
void workerIFdd<K,T>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyI(func, op,  (workerIFdd<L,char> *) dest); break;
		case Int:      _applyI(func, op,  (workerIFdd<L,int> *) dest); break;
		case LongInt:  _applyI(func, op,  (workerIFdd<L,long int> *) dest); break;
		case Float:    _applyI(func, op,  (workerIFdd<L,float> *) dest); break;
		case Double:   _applyI(func, op,  (workerIFdd<L,double> *) dest); break;
		case CharP:    _applyIP(func, op, (workerIFdd<L,char *> *) dest); break;
		case IntP:     _applyIP(func, op, (workerIFdd<L,int *> *) dest); break;
		case LongIntP: _applyIP(func, op, (workerIFdd<L,long int *> *) dest); break;
		case FloatP:   _applyIP(func, op, (workerIFdd<L,float *> *) dest); break;
		case DoubleP:  _applyIP(func, op, (workerIFdd<L,double *> *) dest); break;
		case String:   _applyI(func, op,  (workerIFdd<L,std::string> *) dest); break;
		//case Custom:  _applyI(func, op, (workerIFdd<L,void *> *) dest); break;
		case CharV:   _applyI(func, op, (workerIFdd<L,std::vector<char>> *) dest); break;
		case IntV:    _applyI(func, op, (workerIFdd<L,std::vector<int>> *) dest); break;
		case LongIntV:_applyI(func, op, (workerIFdd<L,std::vector<long int>> *) dest); break;
		case FloatV:  _applyI(func, op, (workerIFdd<L,std::vector<float>> *) dest); break;
		case DoubleV: _applyI(func, op, (workerIFdd<L,std::vector<double>> *) dest); break;
	}

}

template <class K, class T>
void workerIFdd<K,T>::_preApply(void * func, fddOpType op, workerFddBase * dest){
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
		//case Custom:  _apply(func, op,  (workerFdd<void *> *) dest); break;
		case CharV:   _apply(func, op,  (workerFdd<std::vector<char>> *) dest); break;
		case IntV:    _apply(func, op,  (workerFdd<std::vector<int>> *) dest); break;
		case LongIntV:_apply(func, op,  (workerFdd<std::vector<long int>> *) dest); break;
		case FloatV:  _apply(func, op,  (workerFdd<std::vector<float>> *) dest); break;
		case DoubleV: _apply(func, op,  (workerFdd<std::vector<double>> *) dest); break;
	}

}
template <typename K, typename T>
void workerIFdd<K,T>::applyDependent(void * func, fddOpType op, workerFddBase * dest){ 
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



