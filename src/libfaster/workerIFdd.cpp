#include <iostream>
#include <tuple>
#include <set>

#include "indexedFddStorageExtern.cpp"
#include "fastComm.h"
#include "_workerIFdd.h"

// REDUCE
template <typename K, typename T>
std::pair<K,T> faster::_workerIFdd<K,T>::reduce (IreduceIFunctionP<K,T> reduceFunc){
	T * d = this->localData->getData();
	std::pair<K,T> result;
	size_t s = this->localData->getSize();
	K * ik = this->localData->getKeys();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::pair<K,T> partResult (ik[tN], d[tN] );

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
std::pair<K,T>  faster::_workerIFdd<K,T>::bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc){
	K * ik = this->localData->getKeys();
	return bulkReduceFunc(ik, (T*) this->localData->getData(), this->localData->getSize());
}



template <typename K, typename T>
void faster::_workerIFdd<K,T>::applyIndependent(void * func, fddOpType op, fastCommBuffer & buffer){ 
	std::pair<K,T> r;

	switch (op){
		case OP_Reduce:
			//std::cerr << "Reduce ";
			r = reduce( ( IreduceIFunctionP<K,T> ) func);
			break;
		case OP_BulkReduce:
			//std::cerr << "BulkReduce ";
			r = bulkReduce( ( IbulkReduceIFunctionP<K,T> ) func);
			break;
	}

	buffer << r;
}



// -------------------------- Public Functions ------------------------ //



template <typename K, typename T>
void faster::_workerIFdd<K,T>::setData(K * keys, T * data, size_t size) {
	this->localData->setData( keys, data, size);
}

template <typename K, typename T>
void faster::_workerIFdd<K,T>::setDataRaw(void * keys, void * data, size_t size){
	this->localData->setDataRaw(keys, data, size);
}


template <typename K, typename T>
void faster::_workerIFdd<K,T>::insert(void * k, void * in, size_t s UNUSED){ 
	this->localData->insert(*(K *) k, *(T*) in);
}
template <typename K, typename T>
void faster::_workerIFdd<K,T>::insertl(void * in){ 
	insert( *(std::deque<std::pair<K,T>>*) in);
}




template <typename K, typename T>
void faster::_workerIFdd<K,T>::insert(K & key, T & in){ 
	this->localData->insert(key, in); 
}

template <typename K, typename T>
void faster::_workerIFdd<K,T>::insert(std::deque< std::pair<K, T> > & in){ 

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( auto it = in.begin(); it != in.end(); it++){
		this->localData->insert(it->first, it->second); 
	}
}



template <typename K, typename T>
void faster::_workerIFdd<K,T>::apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){ 
	if (op & (OP_GENERICMAP)){
		applyDependent(func, op, dest);
	}else{
		applyIndependent(func, op, buffer);
	}
}



template <typename K, typename T>
void faster::_workerIFdd<K,T>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getKeys(), this->localData->getData(), this->localData->getSize());
};



