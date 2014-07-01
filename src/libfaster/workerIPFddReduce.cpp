#include <iostream>
#include <tuple>

#include "workerIFdd.h"
#include "indexedFddStorage.h"

// REDUCE
template <typename K, typename T>
std::tuple<K,T*,size_t> workerIFdd<K,T*>::reduce (size_t & rSize, IPreduceIPFunctionP<K,T> reduceFunc){
	T ** d = localData->getData();
	std::tuple<K,T*,size_t>  resultT;
	size_t s = localData->getSize();
	size_t * ils = localData->getLineSizes();
	K * ik = localData->getKeys();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::tuple<K,T*,size_t>  partResult (ik[tN], d[tN], ils[tN]);
		T * b, * a;
		size_t aSize, bSize;
		K aKey, bKey;

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			aKey = std::get<0>(partResult);
			a = std::get<1>(partResult);
			aSize = std::get<2>(partResult);
			b = d[i];
			bKey = ik[i];
			bSize = ils[i];

			partResult = reduceFunc( aKey, a, aSize, bKey, b, bSize );

			delete [] a;
			delete [] b;
		}
		#pragma omp master
		resultT = partResult;

		#pragma omp barrier

		#pragma omp critical
		if (omp_get_thread_num() != 0){
			aKey = std::get<0>(resultT);
			a = std::get<1>(resultT);
			aSize = std::get<2>(resultT);

			bKey = std::get<0>(partResult);
			b = std::get<1>(partResult);
			bSize = std::get<2>(partResult);

			resultT = reduceFunc(aKey, a, aSize, bKey, b, bSize);

			delete [] a;
			delete [] b;
		}
	}
	//std::cerr << "END ";
	return resultT;
}

template <typename K, typename T>
std::tuple<K,T*,size_t> workerIFdd<K,T*>::bulkReduce (size_t & rSize, IPbulkReduceIPFunctionP<K,T> bulkReduceFunc){
	K * ik = localData->getKeys();
	return bulkReduceFunc(ik, localData->getData(), localData->getLineSizes(), localData->getSize());
}


template <typename K, typename T>
void workerIFdd<K,T*>::applyReduce(void * func, fddOpType op, void * result, size_t & rSize){ 
	switch (op){
		case OP_Reduce:
			result = new std::tuple<K,T,size_t>();
			*((std::tuple<K,T*,size_t> *) result) = reduce(rSize, ( IPreduceIPFunctionP<K,T> ) func);
			std::cerr << "Reduce " ;
			break;
		case OP_BulkReduce:
			result = new std::tuple<K,T*,size_t>();
			*((std::tuple<K,T*,size_t> *) result) = bulkReduce(rSize, ( IPbulkReduceIPFunctionP<K,T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}
}




template class workerIFdd<char, char *>;
template class workerIFdd<char, int *>;
template class workerIFdd<char, long int *>;
template class workerIFdd<char, float *>;
template class workerIFdd<char, double *>;

template class workerIFdd<int, char *>;
template class workerIFdd<int, int *>;
template class workerIFdd<int, long int *>;
template class workerIFdd<int, float *>;
template class workerIFdd<int, double *>;

template class workerIFdd<long int, char *>;
template class workerIFdd<long int, int *>;
template class workerIFdd<long int, long int *>;
template class workerIFdd<long int, float *>;
template class workerIFdd<long int, double *>;

template class workerIFdd<float, char *>;
template class workerIFdd<float, int *>;
template class workerIFdd<float, long int *>;
template class workerIFdd<float, float *>;
template class workerIFdd<float, double *>;

template class workerIFdd<double, char *>;
template class workerIFdd<double, int *>;
template class workerIFdd<double, long int *>;
template class workerIFdd<double, float *>;
template class workerIFdd<double, double *>;

template class workerIFdd<std::string, char *>;
template class workerIFdd<std::string, int *>;
template class workerIFdd<std::string, long int *>;
template class workerIFdd<std::string, float *>;
template class workerIFdd<std::string, double *>;

