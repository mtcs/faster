#include <iostream>
#include <tuple>
#include "indexedFddStorage.h"
#include "workerIFdd.h"
#include "workerFdd.h"



// REDUCE
template <typename K, typename T>
std::pair<K,T> workerIFdd<K,T>::reduce (IreduceIFunctionP<K,T> reduceFunc){
	T * d = localData->getData();
	std::pair<K,T> result;
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::pair<K,T> partResult (ik[tN], d[tN] );

		//#pragma omp master
		//std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			partResult = reduceFunc(partResult.first, partResult.second, ik[i], d[i]);
		}
		#pragma omp master
		result = partResult;
		
		#pragma omp barrier
		
		#pragma omp critical
		if (omp_get_thread_num() != 0){
			result = reduceFunc(result.first, result.second, partResult.first, partResult.second);
		}
	}
	//std::cerr << "END (RESULT: [" << result.first << "] " << result.second << ")";
	return result;
}


template <typename K, typename T>
std::pair<K,T>  workerIFdd<K,T>::bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc){
	K * ik = localData->getKeys();
	return bulkReduceFunc(ik, (T*) localData->getData(), localData->getSize());
}



template <typename K, typename T>
void workerIFdd<K,T>::applyIndependent(void * func, fddOpType op, void *& result, size_t & rSize){ 
	std::pair<K,T> r;

	switch (op){
		case OP_Reduce:
			r = reduce( ( IreduceIFunctionP<K,T> ) func);
			std::cerr << "Reduce ";
			break;
		case OP_BulkReduce:
			r = bulkReduce( ( IbulkReduceIFunctionP<K,T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}

	resultBuffer->reset();
	*resultBuffer << r;
	result = resultBuffer->data();
	rSize = resultBuffer->size();
}



// -------------------------- Public Functions ------------------------ //


template <typename K, typename T>
workerIFdd<K,T>::workerIFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){
	localData = new indexedFddStorage<K,T>();
} 


template <typename K, typename T>
workerIFdd<K,T>::workerIFdd(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
	localData = new indexedFddStorage<K,T>(size);
}


template <typename K, typename T>
workerIFdd<K,T>::~workerIFdd(){
	delete localData;
	delete resultBuffer;
}


template <typename K, typename T>
void workerIFdd<K,T>::setData(K * keys, T * data, size_t size) {
	localData->setData( keys, data, size);
}

template <typename K, typename T>
void workerIFdd<K,T>::setData(void * data UNUSED, size_t size UNUSED){}

template <typename K, typename T>
void workerIFdd<K,T>::setData(void ** data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED){ }

template <typename K, typename T>
void workerIFdd<K,T>::setData(void * keys, void * data, size_t size){
	localData->setData((K*) keys, (T*) data, size);
}

template <typename K, typename T>
void workerIFdd<K,T>::setData(void * keys UNUSED, void ** data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED){ }

template <typename K, typename T>
fddType workerIFdd<K,T>::getType() { 
	return type; 
}

template <typename K, typename T>
fddType workerIFdd<K,T>::getKeyType() { 
	return keyType; 
}


template <typename K, typename T>
T & workerIFdd<K,T>::operator[](size_t address){ 
	return localData->getData()[address]; 
}

template <typename K, typename T>
void * workerIFdd<K,T>::getData(){ 
	return localData->getData(); 
}

template <typename K, typename T>
K * workerIFdd<K,T>::getKeys(){ 
	return localData->getKeys(); 
}

template <typename K, typename T>
size_t workerIFdd<K,T>::getSize(){ 
	return localData->getSize(); 
}

template <typename K, typename T>
size_t workerIFdd<K,T>::itemSize(){ 
	return sizeof(T); 
}

template <typename K, typename T>
size_t workerIFdd<K,T>::baseSize(){ 
	return sizeof(T); 
}

template <typename K, typename T>
void workerIFdd<K,T>::deleteItem(void * item) { 
	delete (T*) item; 
}

template <typename K, typename T>
void insert(K key, T & in){ 
	workerIFdd<K,T>::localData->insert(key, in); 
}

template <typename K, typename T>
void workerIFdd<K,T>::insert(std::list< std::pair<K, T> > & in){ 
	typename std::list< std::pair<K, T> >::iterator it;

	if (localData->getSize() < in.size())
		localData->grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		localData->insert(it->first, it->second); 
}


template <typename K, typename T>
void workerIFdd<K,T>::shrink(){ localData->shrink(); }


template <typename K, typename T>
void workerIFdd<K,T>::apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize){ 
	switch (op){
		case OP_Map:
		case OP_BulkMap:
		case OP_FlatMap:
		case OP_BulkFlatMap:
			applyDependent(func, op, dest);
		case OP_Reduce:
		case OP_BulkReduce:
			applyIndependent(func, op, result, rSize);
	}
}


