#include <iostream>
#include <tuple>
#include "workerFdd.h"
#include "fddStorage.h"
#include "workerIFdd.h"

// MAP
template <typename T>
template <typename U>
void workerFdd<T *>::map (workerFdd<U> & dest, mapPFunctionP<T,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		dest[i] = mapFunc(d[i], ls[i]);
	}
	//std::cerr << "END ";
}

template <typename T>
template <typename U>
void workerFdd<T *>::map (workerFdd<U> & dest, PmapPFunctionP<T,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < localData->getSize(); ++i){
		std::pair<U,size_t> r = mapFunc(d[i], ls[i]);
		dest[i] = r.first; 
		dls[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void workerFdd<T *>::map (workerIFdd<L,U> & dest, ImapPFunctionP<T,L,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(d[i], ls[i]);
		ok[i] = r.first;
		dest[i] = r.second;

	}
	//std::cerr << "END ";
}

template <typename T>
template <typename L, typename U>
void workerFdd<T *>::map (workerIFdd<L,U> & dest, IPmapPFunctionP<T,L,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	L * ok = dest.getKeys();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < localData->getSize(); ++i){
		std::tuple<L,U,size_t> r = mapFunc(d[i], ls[i]);
		ok[i] = std::get<0>(r);
		dest[i] = std::get<1>(r);
		dls[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		


// BulkMap
template <typename T>
template <typename U>
void workerFdd<T *>::bulkMap (workerFdd<U> & dest, bulkMapPFunctionP<T,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();

	bulkMapFunc((U*) dest.getData(), (T **)localData->getData(), ls, s);
}
template <typename T>
template <typename U>
void workerFdd<T *>::bulkMap (workerFdd<U> & dest, PbulkMapPFunctionP<T,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();

	bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (T **)localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkMap (workerIFdd<L,U> & dest, IbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();

	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), (T **)localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkMap (workerIFdd<L,U> & dest, IPbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();

	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), dest.getLineSizes(), (T **)localData->getData(), ls, s);
}


// FlatMap
template <typename T>
template <typename U>
void workerFdd<T *>::flatMap(workerFdd<U> & dest,  flatMapPFunctionP<T,U> flatMapFunc ){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	std::list<U> resultList;

	#pragma omp parallel
	{
		std::list<U> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<U>r = flatMapFunc(d[i], localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest.insert(resultList);
}
template <typename T>
template <typename U>
void workerFdd<T *>::flatMap(workerFdd<U> & dest,  PflatMapPFunctionP<T,U> flatMapFunc ){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	std::list<std::pair<U, size_t>> resultList;

	#pragma omp parallel
	{
		std::list<std::pair<U, size_t>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>>r = flatMapFunc(d[i], localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest.insert(resultList);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::flatMap(workerIFdd<L,U> & dest,  IflatMapPFunctionP<T,L,U> flatMapFunc ){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel
	{
		std::list<std::pair<L,U>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(d[i], localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest.insert(resultList);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::flatMap(workerIFdd<L,U> & dest,  IPflatMapPFunctionP<T,L,U> flatMapFunc ){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	std::list<std::tuple<L,U, size_t>> resultList;

	#pragma omp parallel
	{
		std::list<std::tuple<L,U, size_t>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::tuple<L,U, size_t>>r = flatMapFunc(d[i], localData->getLineSizes()[i]);

			resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
		}

		//Copy result to the FDD array
		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
	}
	dest.insert(resultList);
}

template <typename T>
template <typename U>
void workerFdd<T *>::bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc( result, rSize, (T**) localData->getData(), localData->getLineSizes(), localData->getSize());
	dest.setData(result, rSize);
}
template <typename T>
template <typename U>
void workerFdd<T *>::bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, (T**) localData->getData(), localData->getLineSizes(), localData->getSize());
	dest.setData(result, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rSize, (T**) localData->getData(), localData->getLineSizes(), localData->getSize());
	dest.setData(keys, result, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t * rDataSizes;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rDataSizes, rSize, (T**) localData->getData(), localData->getLineSizes(), localData->getSize());
	dest.setData(keys, (void **) result, rDataSizes, rSize);
}


// REDUCE
template <typename T>
T * workerFdd<T *>::reduce (size_t & rSize, PreducePFunctionP<T> reduceFunc){
	T ** d = localData->getData();
	T * result;
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		std::pair<T *, size_t>  partResult(d[tN], ls[tN] );
		T * a, * b;
		size_t aSize, bSize;

		#pragma omp master
		std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			a = partResult.first;
			b = d[i];
			aSize = partResult.second;
			bSize = ls[i];

			partResult = reduceFunc(a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
		#pragma omp master
		{
			result = partResult.first;
			rSize = partResult.second;
		}

		#pragma omp barrier

		#pragma omp critical
		if (omp_get_thread_num() != 0){
			a = result;
			b = partResult.first;
			aSize = rSize;
			bSize = partResult.second;

			std::pair<T *, size_t> r = reduceFunc(a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
	}
	//std::cerr << "END ";
	return result;
}

template <typename T>
T * workerFdd<T *>::bulkReduce (size_t & rSize, PbulkReducePFunctionP<T> bulkReduceFunc){
	std::pair<T *, size_t> r = bulkReduceFunc((T**) localData->getData(), localData->getLineSizes(), localData->getSize());
	rSize = r.second;
	return r.first;
}
template <typename T>
template <typename U>
void workerFdd<T *>::_apply(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t & rSize){
	switch (op){
		case OP_Map:
			map(*dest, (mapPFunctionP<T,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( bulkMapPFunctionP<T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( flatMapPFunctionP<T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( bulkFlatMapPFunctionP<T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename T>
template <typename U>
void workerFdd<T *>::_applyP(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t & rSize){
	switch (op){
		case OP_Map:
			map(*dest, (PmapPFunctionP<T,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( PbulkMapPFunctionP<T,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( PflatMapPFunctionP<T,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( PbulkFlatMapPFunctionP<T,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
		case OP_Reduce:
			result = reduce(rSize, ( PreducePFunctionP<T> ) func);
			std::cerr << "Reduce " ;
			break;
		case OP_BulkReduce:
			result = bulkReduce(rSize, ( PbulkReducePFunctionP<T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}
}

template <typename T>
template <typename L, typename U>
void workerFdd<T *>::_applyI(void * func, fddOpType op, workerIFdd<L,U> * dest, void * result, size_t & rSize){
	switch (op){
		case OP_Map:
			map(*dest, (ImapPFunctionP<T,L,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IbulkMapPFunctionP<T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IflatMapPFunctionP<T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IbulkFlatMapPFunctionP<T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::_applyIP(void * func, fddOpType op, workerIFdd<L,U> * dest, void * result, size_t & rSize){
	switch (op){
		case OP_Map:
			map(*dest, (IPmapPFunctionP<T,L,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IPbulkMapPFunctionP<T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap(*dest, ( IPflatMapPFunctionP<T,L,U> ) func);
			std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap(*dest, ( IPbulkFlatMapPFunctionP<T,L,U> ) func);
			std::cerr << "BulkFlatMap ";
			break;
	}
}

template <typename T>
void workerFdd<T *>::_preApply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _apply(func, op, (workerFdd<char> *) dest, result, rSize); break;
		case Int:      _apply(func, op, (workerFdd<int> *) dest, result, rSize); break;
		case LongInt:  _apply(func, op, (workerFdd<long int> *) dest, result, rSize); break;
		case Float:    _apply(func, op, (workerFdd<float> *) dest, result, rSize); break;
		case Double:   _apply(func, op, (workerFdd<double> *) dest, result, rSize); break;
		case CharP:    _applyP(func, op, (workerFdd<char *> *) dest, result, rSize); break;
		case IntP:     _applyP(func, op, (workerFdd<int *> *) dest, result, rSize); break;
		case LongIntP: _applyP(func, op, (workerFdd<long int *> *) dest, result, rSize); break;
		case FloatP:   _applyP(func, op, (workerFdd<float *> *) dest, result, rSize); break;
		case DoubleP:  _applyP(func, op, (workerFdd<double *> *) dest, result, rSize); break;
		case String:   _apply(func, op, (workerFdd<std::string> *) dest, result, rSize); break;
		//case Custom:   _apply(func, op, (workerFdd<void *> *) dest, result, rSize); break;
		//case CharV:     _apply(func, op, (workerFdd<std::vector<char>> *) dest, result, rSize); break;
		//case IntV:      _apply(func, op, (workerFdd<std::vector<int>> *) dest, result, rSize); break;
		//case LongIntV:  _apply(func, op, (workerFdd<std::vector<long int>> *) dest, result, rSize); break;
		//case FloatV:    _apply(func, op, (workerFdd<std::vector<float>> *) dest, result, rSize); break;
		//case DoubleV:   _apply(func, op, (workerFdd<std::vector<double>> *) dest, result, rSize); break;
	}
}

template <typename T>
template <typename L>
void workerFdd<T *>::_preApplyI(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyI(func, op,  (workerIFdd<L, char> *) dest, result, rSize); break;
		case Int:      _applyI(func, op,  (workerIFdd<L, int> *) dest, result, rSize); break;
		case LongInt:  _applyI(func, op,  (workerIFdd<L, long int> *) dest, result, rSize); break;
		case Float:    _applyI(func, op,  (workerIFdd<L, float> *) dest, result, rSize); break;
		case Double:   _applyI(func, op,  (workerIFdd<L, double> *) dest, result, rSize); break;
		case CharP:    _applyIP(func, op, (workerIFdd<L, char *> *) dest, result, rSize); break;
		case IntP:     _applyIP(func, op, (workerIFdd<L, int *> *) dest, result, rSize); break;
		case LongIntP: _applyIP(func, op, (workerIFdd<L, long int *> *) dest, result, rSize); break;
		case FloatP:   _applyIP(func, op, (workerIFdd<L, float *> *) dest, result, rSize); break;
		case DoubleP:  _applyIP(func, op, (workerIFdd<L, double *> *) dest, result, rSize); break;
		case String:   _applyI(func, op,  (workerIFdd<L, std::string> *) dest, result, rSize); break;
		//case Custom:   _applyI(func, op, (workerFdd<L, void *> *) dest, result, rSize); break;
		//case CharV:     _applyI(func, op, (workerFdd<L, std::vector<char>> *) dest, result, rSize); break;
		//case IntV:      _applyI(func, op, (workerFdd<L, std::vector<int>> *) dest, result, rSize); break;
		//case LongIntV:  _applyI(func, op, (workerFdd<L, std::vector<long int>> *) dest, result, rSize); break;
		//case FloatV:    _applyI(func, op, (workerFdd<L, std::vector<float>> *) dest, result, rSize); break;
		//case DoubleV:   _applyI(func, op, (workerFdd<L, std::vector<double>> *) dest, result, rSize); break;
	}
}

template <typename T>
void workerFdd<T *>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (dest->getKeyType()){
		case Null:     _preApply(func, op, dest, result, rSize);break;
		case Char:     _preApplyI<char>(func, op, dest, result, rSize); break;
		case Int:      _preApplyI<int>(func, op, dest, result, rSize); break;
		case LongInt:  _preApplyI<long int>(func, op, dest, result, rSize); break;
		case Float:    _preApplyI<float>(func, op, dest, result, rSize); break;
		case Double:   _preApplyI<double>(func, op, dest, result, rSize); break;
		case String:   _preApplyI<std::string>(func, op, dest, result, rSize); break;
	}
}

template class workerFdd<char *>;
template class workerFdd<int *>;
template class workerFdd<long int *>;
template class workerFdd<float *>;
template class workerFdd<double *>;

