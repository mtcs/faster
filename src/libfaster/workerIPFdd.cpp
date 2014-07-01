#include <tuple>
#include <iostream>

#include "workerIFdd.h"
#include "workerFdd.h"
#include "indexedFddStorage.h"

// -------------------------- worker<K,T*> specialization -------------------------- //

template <typename K, typename T>
void workerIFdd<K,T*>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (op){
		case OP_Map:
		case OP_BulkMap:
			applyMap(func, op, dest);
		case OP_FlatMap:
		case OP_BulkFlatMap:
			applyFlatMap(func, op, dest);
		case OP_Reduce:
		case OP_BulkReduce:
			applyReduce(func, op, result, rSize);
	}
}


// -------------------------- Public Functions ------------------------ //

template <typename K, typename T>
void workerIFdd<K,T*>::insert(K key, T* & in, size_t s){ 
	localData->insert(key, in, s); 
}

template <typename K, typename T>
void workerIFdd<K,T*>::insert(std::list< std::tuple<K, T*, size_t> > & in){ 
	typename std::list< std::tuple<K, T*, size_t> >::iterator it;

	if (localData->getSize() < in.size())
		localData->grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		localData->insert(std::get<0>(*it), std::get<1>(*it), std::get<2>(*it)); 
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


