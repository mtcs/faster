#include <iostream>
#include <tuple>
#include <set>

#include "indexedFddStorageExtern.cpp"
#include "fastComm.h"
#include "workerIFdd.h"

// REDUCE
template <typename K, typename T>
std::pair<K,T> workerIFdd<K,T>::reduce (IreduceIFunctionP<K,T> reduceFunc){
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
std::pair<K,T>  workerIFdd<K,T>::bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc){
	K * ik = this->localData->getKeys();
	return bulkReduceFunc(ik, (T*) this->localData->getData(), this->localData->getSize());
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

	this->resultBuffer->reset();
	*this->resultBuffer << r;
	result = this->resultBuffer->data();
	rSize = this->resultBuffer->size();
}



// -------------------------- Public Functions ------------------------ //



template <typename K, typename T>
void workerIFdd<K,T>::setData(K * keys, T * data, size_t size) {
	this->localData->setData( keys, data, size);
}

template <typename K, typename T>
void workerIFdd<K,T>::setDataRaw(void * keys, void * data, size_t size){
	this->localData->setDataRaw(keys, data, size);
}



template <typename K, typename T>
void workerIFdd<K,T>::insert(K key, T & in){ 
	this->localData->insert(key, in); 
}

template <typename K, typename T>
void workerIFdd<K,T>::insert(std::list< std::pair<K, T> > & in){ 

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( auto it = in.begin(); it != in.end(); it++)
		this->localData->insert(it->first, it->second); 
}



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



template <typename K, typename T>
void workerIFdd<K,T>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getKeys(), this->localData->getData(), this->localData->getSize());
};



template class workerIFdd<char, char>;
template class workerIFdd<char, int>;
template class workerIFdd<char, long int>;
template class workerIFdd<char, float>;
template class workerIFdd<char, double>;
extern template class workerIFdd<char, char*>;
extern template class workerIFdd<char, int*>;
extern template class workerIFdd<char, long int*>;
extern template class workerIFdd<char, float*>;
extern template class workerIFdd<char, double*>;
template class workerIFdd<char, std::string>;
template class workerIFdd<char, std::vector<char>>;
template class workerIFdd<char, std::vector<int>>;
template class workerIFdd<char, std::vector<long int>>;
template class workerIFdd<char, std::vector<float>>;
template class workerIFdd<char, std::vector<double>>;

template class workerIFdd<int, char>;
template class workerIFdd<int, int>;
template class workerIFdd<int, long int>;
template class workerIFdd<int, float>;
template class workerIFdd<int, double>;
extern template class workerIFdd<int, char*>;
extern template class workerIFdd<int, int*>;
extern template class workerIFdd<int, long int*>;
extern template class workerIFdd<int, float*>;
extern template class workerIFdd<int, double*>;
template class workerIFdd<int, std::string>;
template class workerIFdd<int, std::vector<char>>;
template class workerIFdd<int, std::vector<int>>;
template class workerIFdd<int, std::vector<long int>>;
template class workerIFdd<int, std::vector<float>>;
template class workerIFdd<int, std::vector<double>>;

template class workerIFdd<long int, char>;
template class workerIFdd<long int, int>;
template class workerIFdd<long int, long int>;
template class workerIFdd<long int, float>;
template class workerIFdd<long int, double>;
extern template class workerIFdd<long int, char*>;
extern template class workerIFdd<long int, int*>;
extern template class workerIFdd<long int, long int*>;
extern template class workerIFdd<long int, float*>;
extern template class workerIFdd<long int, double*>;
template class workerIFdd<long int, std::string>;
template class workerIFdd<long, std::vector<char>>;
template class workerIFdd<long, std::vector<int>>;
template class workerIFdd<long, std::vector<long int>>;
template class workerIFdd<long, std::vector<float>>;
template class workerIFdd<long, std::vector<double>>;

template class workerIFdd<float, char>;
template class workerIFdd<float, int>;
template class workerIFdd<float, long int>;
template class workerIFdd<float, float>;
template class workerIFdd<float, double>;
extern template class workerIFdd<float, char*>;
extern template class workerIFdd<float, int*>;
extern template class workerIFdd<float, long int*>;
extern template class workerIFdd<float, float*>;
extern template class workerIFdd<float, double*>;
template class workerIFdd<float, std::string>;
template class workerIFdd<float, std::vector<char>>;
template class workerIFdd<float, std::vector<int>>;
template class workerIFdd<float, std::vector<long int>>;
template class workerIFdd<float, std::vector<float>>;
template class workerIFdd<float, std::vector<double>>;

template class workerIFdd<double, char>;
template class workerIFdd<double, int>;
template class workerIFdd<double, long int>;
template class workerIFdd<double, float>;
template class workerIFdd<double, double>;
extern template class workerIFdd<double, char*>;
extern template class workerIFdd<double, int*>;
extern template class workerIFdd<double, long int*>;
extern template class workerIFdd<double, float*>;
extern template class workerIFdd<double, double*>;
template class workerIFdd<double, std::string>;
template class workerIFdd<double, std::vector<char>>;
template class workerIFdd<double, std::vector<int>>;
template class workerIFdd<double, std::vector<long int>>;
template class workerIFdd<double, std::vector<float>>;
template class workerIFdd<double, std::vector<double>>;

template class workerIFdd<std::string, char>;
template class workerIFdd<std::string, int>;
template class workerIFdd<std::string, long int>;
template class workerIFdd<std::string, float>;
template class workerIFdd<std::string, double>;
extern template class workerIFdd<std::string, char*>;
extern template class workerIFdd<std::string, int*>;
extern template class workerIFdd<std::string, long int*>;
extern template class workerIFdd<std::string, float*>;
extern template class workerIFdd<std::string, double*>;
template class workerIFdd<std::string, std::string>;
template class workerIFdd<std::string, std::vector<char>>;
template class workerIFdd<std::string, std::vector<int>>;
template class workerIFdd<std::string, std::vector<long int>>;
template class workerIFdd<std::string, std::vector<float>>;
template class workerIFdd<std::string, std::vector<double>>;
