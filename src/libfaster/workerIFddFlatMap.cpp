#include <iostream>
#include "workerIFdd.h"



template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::flatMap(workerIFdd<L,U> & dest,  IflatMapIFunctionP<K,T,L,U> flatMapFunc ){
	size_t s = localData.getSize();
	K * ik = localData.getKeys();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<L,U>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(ik[i], localData[i]);

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
	size_t s = localData.getSize();
	K * ik = localData.getKeys();
	std::list< std::tuple<L, U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::tuple<L, U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::tuple<L, U, size_t>> r = flatMapFunc(ik[i], localData[i]);

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
	size_t s = localData.getSize();
	K * ik = localData.getKeys();
	std::list<U> resultList;

	#pragma omp parallel 
	{
		std::list<U> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<U> r = flatMapFunc(ik[i], localData[i]);

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
	size_t s = localData.getSize();
	K * ik = localData.getKeys();
	std::list< std::pair<U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<U, size_t>> partResultList;

		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>> r = flatMapFunc(ik[i], localData[i]);

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

	bulkFlatMapFunc(ok, result, rSize, localData.getKeys(), localData.getData(), localData.getSize());
	dest.setData(ok, result, rSize);
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T>::bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc(ok, result, rDataSizes, rSize, localData.getKeys(), (T*) localData.getData(), localData.getSize());
	dest.setData( ok, (void**) result, rDataSizes, rSize);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc(result, rSize, localData.getKeys(), localData.getData(), localData.getSize());
	dest.setData(result, rSize);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T>::bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, localData.getKeys(), (T*) localData.getData(), localData.getSize());
	dest.setData( (void**) result, rDataSizes, rSize);
}


// Not Pointer -> Not Pointer
template <class K, class T>
template <typename L, typename U>
void workerIFdd<K,T>::_applyIFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
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
void workerIFdd<K,T>::_applyIPFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
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
void workerIFdd<K,T>::_applyFlatMap(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
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
void workerIFdd<K,T>::_applyPFlatMap(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
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
void workerIFdd<K,T>::_preApplyIFlatMap(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyIFlatMap(func, op,  (workerIFdd<L,char> *) dest); break;
		case Int:      _applyIFlatMap(func, op,  (workerIFdd<L,int> *) dest); break;
		case LongInt:  _applyIFlatMap(func, op,  (workerIFdd<L,long int> *) dest); break;
		case Float:    _applyIFlatMap(func, op,  (workerIFdd<L,float> *) dest); break;
		case Double:   _applyIFlatMap(func, op,  (workerIFdd<L,double> *) dest); break;
		case CharP:    _applyIPFlatMap(func, op, (workerIFdd<L,char *> *) dest); break;
		case IntP:     _applyIPFlatMap(func, op, (workerIFdd<L,int *> *) dest); break;
		case LongIntP: _applyIPFlatMap(func, op, (workerIFdd<L,long int *> *) dest); break;
		case FloatP:   _applyIPFlatMap(func, op, (workerIFdd<L,float *> *) dest); break;
		case DoubleP:  _applyIPFlatMap(func, op, (workerIFdd<L,double *> *) dest); break;
		case String:   _applyIFlatMap(func, op,  (workerIFdd<L,std::string> *) dest); break;
		//case Custom:  _applyIFlatMap(func, op, (workerIFdd<L,void *> *) dest); break;
		//case CharV:   _applyIFlatMap(func, op, (workerIFdd<L,std::vector<char>> *) dest); break;
		//case IntV:    _applyIFlatMap(func, op, (workerIFdd<L,std::vector<int>> *) dest); break;
		//case LongIntV:_applyIFlatMap(func, op, (workerIFdd<L,std::vector<long int>> *) dest); break;
		//case FloatV:  _applyIFlatMap(func, op, (workerIFdd<L,std::vector<float>> *) dest); break;
		//case DoubleV: _applyIFlatMap(func, op, (workerIFdd<L,std::vector<double>> *) dest); break;
	}

}

template <class K, class T>
void workerIFdd<K,T>::_preApplyFlatMap(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyFlatMap(func, op,  (workerFdd<char> *) dest); break;
		case Int:      _applyFlatMap(func, op,  (workerFdd<int> *) dest); break;
		case LongInt:  _applyFlatMap(func, op,  (workerFdd<long int> *) dest); break;
		case Float:    _applyFlatMap(func, op,  (workerFdd<float> *) dest); break;
		case Double:   _applyFlatMap(func, op,  (workerFdd<double> *) dest); break;
		case CharP:    _applyPFlatMap(func, op, (workerFdd<char *> *) dest); break;
		case IntP:     _applyPFlatMap(func, op, (workerFdd<int *> *) dest); break;
		case LongIntP: _applyPFlatMap(func, op, (workerFdd<long int *> *) dest); break;
		case FloatP:   _applyPFlatMap(func, op, (workerFdd<float *> *) dest); break;
		case DoubleP:  _applyPFlatMap(func, op, (workerFdd<double *> *) dest); break;
		case String:   _applyFlatMap(func, op,  (workerFdd<std::string> *) dest); break;
		//case Custom:  _applyFlatMap(func, op,  (workerFdd<void *> *) dest); break;
		//case CharV:   _applyFlatMap(func, op,  (workerFdd<std::vector<char>> *) dest); break;
		//case IntV:    _applyFlatMap(func, op,  (workerFdd<std::vector<int>> *) dest); break;
		//case LongIntV:_applyFlatMap(func, op,  (workerFdd<std::vector<long int>> *) dest); break;
		//case FloatV:  _applyFlatMap(func, op,  (workerFdd<std::vector<float>> *) dest); break;
		//case DoubleV: _applyFlatMap(func, op,  (workerFdd<std::vector<double>> *) dest); break;
	}

}
template <typename K, typename T>
void workerIFdd<K,T>::applyFlatMap(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getKeyType()){
		case Null:     _preApplyFlatMap(func, op, dest);break;
		case Char:     _preApplyIFlatMap<char>(func, op, dest); break;
		case Int:      _preApplyIFlatMap<int>(func, op, dest); break;
		case LongInt:  _preApplyIFlatMap<long int>(func, op, dest); break;
		case Float:    _preApplyIFlatMap<float>(func, op, dest); break;
		case Double:   _preApplyIFlatMap<double>(func, op, dest); break;
		case String:   _preApplyIFlatMap<std::string>(func, op, dest); break;
	}
}



