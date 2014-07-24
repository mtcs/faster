#include <iostream>
#include <tuple>
#include <set>

#include "indexedFddStorageExtern.cpp"
#include "fastComm.h"
#include "workerIFdd.h"

// REDUCE
template <typename K, typename T>
std::pair<K,T> faster::workerIFdd<K,T>::reduce (IreduceIFunctionP<K,T> reduceFunc){
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
std::pair<K,T>  faster::workerIFdd<K,T>::bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc){
	K * ik = this->localData->getKeys();
	return bulkReduceFunc(ik, (T*) this->localData->getData(), this->localData->getSize());
}



template <typename K, typename T>
void faster::workerIFdd<K,T>::applyIndependent(void * func, fddOpType op, void *& result, size_t & rSize){ 
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

	this->resultBuffer->reset();
	*this->resultBuffer << r;
	result = this->resultBuffer->data();
	rSize = this->resultBuffer->size();
}



// -------------------------- Public Functions ------------------------ //



template <typename K, typename T>
void faster::workerIFdd<K,T>::setData(K * keys, T * data, size_t size) {
	this->localData->setData( keys, data, size);
}

template <typename K, typename T>
void faster::workerIFdd<K,T>::setDataRaw(void * keys, void * data, size_t size){
	this->localData->setDataRaw(keys, data, size);
}



template <typename K, typename T>
void faster::workerIFdd<K,T>::insert(K & key, T & in){ 
	this->localData->insert(key, in); 
}

template <typename K, typename T>
void faster::workerIFdd<K,T>::insert(std::list< std::pair<K, T> > & in){ 

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( auto it = in.begin(); it != in.end(); it++)
		this->localData->insert(it->first, it->second); 
}



template <typename K, typename T>
void faster::workerIFdd<K,T>::apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize){ 
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



template <typename K, typename T>
void faster::workerIFdd<K,T>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getKeys(), this->localData->getData(), this->localData->getSize());
};



template class faster::workerIFdd<char, char>;
template class faster::workerIFdd<char, int>;
template class faster::workerIFdd<char, long int>;
template class faster::workerIFdd<char, float>;
template class faster::workerIFdd<char, double>;
extern template class faster::workerIFdd<char, char*>;
extern template class faster::workerIFdd<char, int*>;
extern template class faster::workerIFdd<char, long int*>;
extern template class faster::workerIFdd<char, float*>;
extern template class faster::workerIFdd<char, double*>;
template class faster::workerIFdd<char, std::string>;
template class faster::workerIFdd<char, std::vector<char>>;
template class faster::workerIFdd<char, std::vector<int>>;
template class faster::workerIFdd<char, std::vector<long int>>;
template class faster::workerIFdd<char, std::vector<float>>;
template class faster::workerIFdd<char, std::vector<double>>;

template class faster::workerIFdd<int, char>;
template class faster::workerIFdd<int, int>;
template class faster::workerIFdd<int, long int>;
template class faster::workerIFdd<int, float>;
template class faster::workerIFdd<int, double>;
extern template class faster::workerIFdd<int, char*>;
extern template class faster::workerIFdd<int, int*>;
extern template class faster::workerIFdd<int, long int*>;
extern template class faster::workerIFdd<int, float*>;
extern template class faster::workerIFdd<int, double*>;
template class faster::workerIFdd<int, std::string>;
template class faster::workerIFdd<int, std::vector<char>>;
template class faster::workerIFdd<int, std::vector<int>>;
template class faster::workerIFdd<int, std::vector<long int>>;
template class faster::workerIFdd<int, std::vector<float>>;
template class faster::workerIFdd<int, std::vector<double>>;

template class faster::workerIFdd<long int, char>;
template class faster::workerIFdd<long int, int>;
template class faster::workerIFdd<long int, long int>;
template class faster::workerIFdd<long int, float>;
template class faster::workerIFdd<long int, double>;
extern template class faster::workerIFdd<long int, char*>;
extern template class faster::workerIFdd<long int, int*>;
extern template class faster::workerIFdd<long int, long int*>;
extern template class faster::workerIFdd<long int, float*>;
extern template class faster::workerIFdd<long int, double*>;
template class faster::workerIFdd<long int, std::string>;
template class faster::workerIFdd<long, std::vector<char>>;
template class faster::workerIFdd<long, std::vector<int>>;
template class faster::workerIFdd<long, std::vector<long int>>;
template class faster::workerIFdd<long, std::vector<float>>;
template class faster::workerIFdd<long, std::vector<double>>;

template class faster::workerIFdd<float, char>;
template class faster::workerIFdd<float, int>;
template class faster::workerIFdd<float, long int>;
template class faster::workerIFdd<float, float>;
template class faster::workerIFdd<float, double>;
extern template class faster::workerIFdd<float, char*>;
extern template class faster::workerIFdd<float, int*>;
extern template class faster::workerIFdd<float, long int*>;
extern template class faster::workerIFdd<float, float*>;
extern template class faster::workerIFdd<float, double*>;
template class faster::workerIFdd<float, std::string>;
template class faster::workerIFdd<float, std::vector<char>>;
template class faster::workerIFdd<float, std::vector<int>>;
template class faster::workerIFdd<float, std::vector<long int>>;
template class faster::workerIFdd<float, std::vector<float>>;
template class faster::workerIFdd<float, std::vector<double>>;

template class faster::workerIFdd<double, char>;
template class faster::workerIFdd<double, int>;
template class faster::workerIFdd<double, long int>;
template class faster::workerIFdd<double, float>;
template class faster::workerIFdd<double, double>;
extern template class faster::workerIFdd<double, char*>;
extern template class faster::workerIFdd<double, int*>;
extern template class faster::workerIFdd<double, long int*>;
extern template class faster::workerIFdd<double, float*>;
extern template class faster::workerIFdd<double, double*>;
template class faster::workerIFdd<double, std::string>;
template class faster::workerIFdd<double, std::vector<char>>;
template class faster::workerIFdd<double, std::vector<int>>;
template class faster::workerIFdd<double, std::vector<long int>>;
template class faster::workerIFdd<double, std::vector<float>>;
template class faster::workerIFdd<double, std::vector<double>>;

template class faster::workerIFdd<std::string, char>;
template class faster::workerIFdd<std::string, int>;
template class faster::workerIFdd<std::string, long int>;
template class faster::workerIFdd<std::string, float>;
template class faster::workerIFdd<std::string, double>;
extern template class faster::workerIFdd<std::string, char*>;
extern template class faster::workerIFdd<std::string, int*>;
extern template class faster::workerIFdd<std::string, long int*>;
extern template class faster::workerIFdd<std::string, float*>;
extern template class faster::workerIFdd<std::string, double*>;
template class faster::workerIFdd<std::string, std::string>;
template class faster::workerIFdd<std::string, std::vector<char>>;
template class faster::workerIFdd<std::string, std::vector<int>>;
template class faster::workerIFdd<std::string, std::vector<long int>>;
template class faster::workerIFdd<std::string, std::vector<float>>;
template class faster::workerIFdd<std::string, std::vector<double>>;
