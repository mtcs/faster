#include <tuple>
#include <iostream>

#include "workerIFdd.h"
#include "workerFdd.h"
#include "indexedFddStorage.h"

// -------------------------- worker<K,T*> specialization -------------------------- //

// REDUCE
template <typename K, typename T>
std::tuple<K,T*,size_t> workerIFdd<K,T*>::reduce (IPreduceIPFunctionP<K,T> reduceFunc){
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
std::tuple<K,T*,size_t> workerIFdd<K,T*>::bulkReduce (IPbulkReduceIPFunctionP<K,T> bulkReduceFunc){
	K * ik = localData->getKeys();
	return bulkReduceFunc(ik, localData->getData(), localData->getLineSizes(), localData->getSize());
}


template <typename K, typename T>
void workerIFdd<K,T*>::applyIndependent(void * func, fddOpType op, void * result, size_t & rSize){ 
	std::tuple<K,T*,size_t> r;

	switch (op){
		case OP_Reduce:
			r = reduce(( IPreduceIPFunctionP<K,T> ) func);
			std::cerr << "Reduce " ;
			break;
		case OP_BulkReduce:
			r = bulkReduce(( IPbulkReduceIPFunctionP<K,T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}
	
	resultBuffer->reset();
	*resultBuffer << r;
	result = resultBuffer->data();
	rSize = resultBuffer->size();
}


template <typename K, typename T>
void workerIFdd<K,T*>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
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

