#include <iostream>
#include "workerIFdd.h"
#include "indexedFddStorage.h"

// REDUCE
template <typename K, typename T>
std::pair<K,T> workerIFdd<K,T>::reduce (IreduceIFunctionP<K,T> reduceFunc){
	T * d = localData->getData();
	std::pair<K,T> result;
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::pair<K,T> partResult (ik[tN], d[tN] );

		#pragma omp master
		std::cerr << tN << "(" << nT << ")";

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
	std::cerr << "END (RESULT: [" << result.first << "] " << result.second << ")";
	return result;
}

template <typename K, typename T>
std::pair<K,T>  workerIFdd<K,T>::bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc){
	K * ik = localData->getKeys();
	return bulkReduceFunc(ik, (T*) localData->getData(), localData->getSize());
}

template <typename K, typename T>
void workerIFdd<K,T>::applyReduce(void * func, fddOpType op, void * result, size_t & rSize){ 
	switch (op){
		case OP_Reduce:
			result = new std::pair<K,T>;
			*((std::pair<K,T> *) result) = reduce( ( IreduceIFunctionP<K,T> ) func);
			std::cerr << "Reduce ";
			break;
		case OP_BulkReduce:
			result = new std::pair<K,T*>;
			*((std::pair<K,T> *) result) = bulkReduce( ( IbulkReduceIFunctionP<K,T> ) func);

			std::cerr << "BulkReduce ";
			break;
	}
}




template class workerIFdd<char, char>;
template class workerIFdd<char, int>;
template class workerIFdd<char, long int>;
template class workerIFdd<char, float>;
template class workerIFdd<char, double>;
template class workerIFdd<char, std::string>;
//template class workerIFdd<char, std::vector<char>>;
//template class workerIFdd<char, std::vector<int>>;
//template class workerIFdd<char, std::vector<long int>>;
//template class workerIFdd<char, std::vector<float>>;
//template class workerIFdd<char, std::vector<double>>;

template class workerIFdd<int, char>;
template class workerIFdd<int, int>;
template class workerIFdd<int, long int>;
template class workerIFdd<int, float>;
template class workerIFdd<int, double>;
template class workerIFdd<int, std::string>;
//template class workerIFdd<int, std::vector<char>>;
//template class workerIFdd<int, std::vector<int>>;
//template class workerIFdd<int, std::vector<long int>>;
//template class workerIFdd<int, std::vector<float>>;
//template class workerIFdd<int, std::vector<double>>;

template class workerIFdd<long int, char>;
template class workerIFdd<long int, int>;
template class workerIFdd<long int, long int>;
template class workerIFdd<long int, float>;
template class workerIFdd<long int, double>;
template class workerIFdd<long int, std::string>;
//template class workerIFdd<long int, std::vector<char>>;
//template class workerIFdd<long int, std::vector<int>>;
//template class workerIFdd<long int, std::vector<long int>>;
//template class workerIFdd<long int, std::vector<float>>;
//template class workerIFdd<long int, std::vector<double>>;

template class workerIFdd<float, char>;
template class workerIFdd<float, int>;
template class workerIFdd<float, long int>;
template class workerIFdd<float, float>;
template class workerIFdd<float, double>;
template class workerIFdd<float, std::string>;
//template class workerIFdd<float, std::vector<char>>;
//template class workerIFdd<float, std::vector<int>>;
//template class workerIFdd<float, std::vector<long int>>;
//template class workerIFdd<float, std::vector<float>>;
//template class workerIFdd<float, std::vector<double>>;

template class workerIFdd<double, char>;
template class workerIFdd<double, int>;
template class workerIFdd<double, long int>;
template class workerIFdd<double, float>;
template class workerIFdd<double, double>;
template class workerIFdd<double, std::string>;
//template class workerIFdd<double, std::vector<char>>;
//template class workerIFdd<double, std::vector<int>>;
//template class workerIFdd<double, std::vector<long int>>;
//template class workerIFdd<double, std::vector<float>>;
//template class workerIFdd<double, std::vector<double>>;

template class workerIFdd<std::string, char>;
template class workerIFdd<std::string, int>;
template class workerIFdd<std::string, long int>;
template class workerIFdd<std::string, float>;
template class workerIFdd<std::string, double>;
template class workerIFdd<std::string, std::string>;
//template class workerIFdd<std::string, std::vector<char>>;
//template class workerIFdd<std::string, std::vector<int>>;
//template class workerIFdd<std::string, std::vector<long int>>;
//template class workerIFdd<std::string, std::vector<float>>;
//template class workerIFdd<std::string, std::vector<double>>;

