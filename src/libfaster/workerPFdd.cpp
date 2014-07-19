#include <iostream>
#include <tuple>
#include "workerFdd.h"
#include "fddStorageExtern.cpp"
#include "workerIFddExtern.cpp"
#include "fastComm.h"

// MAP
template <typename T>
template <typename U>
void workerFdd<T *>::map (workerFdd<U> & dest, mapPFunctionP<T,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

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
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<U,size_t> r = mapFunc(d[i], ls[i]);
		dest[i] = r.first; 
		dls[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename T>
template <typename L, typename U>
void workerFdd<T *>::map (workerIFdd<L,U> & dest, ImapPFunctionP<T,L,U> mapFunc){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
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
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	L * ok = dest.getKeys();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
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
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((U*) dest.getData(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename U>
void workerFdd<T *>::bulkMap (workerFdd<U> & dest, PbulkMapPFunctionP<T,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkMap (workerIFdd<L,U> & dest, IbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), (T **)this->localData->getData(), ls, s);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkMap (workerIFdd<L,U> & dest, IPbulkMapPFunctionP<T,L,U> bulkMapFunc){
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();

	bulkMapFunc(dest.getKeys(), (U*) dest.getData(), dest.getLineSizes(), (T **)this->localData->getData(), ls, s);
}


// FlatMap
template <typename T>
template <typename U>
void workerFdd<T *>::flatMap(workerFdd<U> & dest,  flatMapPFunctionP<T,U> flatMapFunc ){
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::list<U> resultList;

	#pragma omp parallel
	{
		std::list<U> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<U>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

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
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::list<std::pair<U, size_t>> resultList;

	#pragma omp parallel
	{
		std::list<std::pair<U, size_t>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

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
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel
	{
		std::list<std::pair<L,U>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

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
	T ** d = this->localData->getData();
	size_t s = this->localData->getSize();
	std::list<std::tuple<L,U, size_t>> resultList;

	#pragma omp parallel
	{
		std::list<std::tuple<L,U, size_t>> partResultList;
		#pragma omp for 
		for (int i = 0; i < s; ++i){
			std::list<std::tuple<L,U, size_t>>r = flatMapFunc(d[i], this->localData->getLineSizes()[i]);

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

	bulkFlatMapFunc( result, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest.setData(result, rSize);
}
template <typename T>
template <typename U>
void workerFdd<T *>::bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest.setData(result, rDataSizes, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest.setData(keys, result, rSize);
}
template <typename T>
template <typename L, typename U>
void workerFdd<T *>::bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc ){
	L * keys;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( keys, result, rDataSizes, rSize, (T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	dest.setData(keys, result, rDataSizes, rSize);
}


// REDUCE
template <typename T>
std::pair<T*,size_t>  workerFdd<T *>::reduce (PreducePFunctionP<T> reduceFunc){
	T ** d = this->localData->getData();
	std::pair<T*,size_t> result;
	size_t s = this->localData->getSize();
	size_t * ls = this->localData->getLineSizes();
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
			result = partResult;
		}

		#pragma omp barrier

		#pragma omp critical
		if (omp_get_thread_num() != 0){
			a = result.first;
			b = partResult.first;
			aSize = result.second;
			bSize = partResult.second;

			result = reduceFunc(a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
	}
	//std::cerr << "END ";
	return result;
}

template <typename T>
std::pair<T*,size_t> workerFdd<T *>::bulkReduce (PbulkReducePFunctionP<T> bulkReduceFunc){
	std::pair<T *, size_t> r = bulkReduceFunc((T**) this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
	return r;
}
template <typename T>
template <typename U>
void workerFdd<T *>::_apply(void * func, fddOpType op, workerFdd<U> * dest){
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
void workerFdd<T *>::_applyP(void * func, fddOpType op, workerFdd<U> * dest){
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
	}
}

template <typename T>
template <typename L, typename U>
void workerFdd<T *>::_applyI(void * func, fddOpType op, workerIFdd<L,U> * dest){
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
void workerFdd<T *>::_applyIP(void * func, fddOpType op, workerIFdd<L,U> * dest){
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
void workerFdd<T *>::_applyReduce(void * func, fddOpType op, void *& result, size_t & rSize){
	std::pair<T*,size_t> r;
	switch (op){
		case OP_Reduce:
			r = reduce(( PreducePFunctionP<T> ) func);
			std::cerr << "Reduce " ;
			break;
		case OP_BulkReduce:
			r = bulkReduce(( PbulkReducePFunctionP<T> ) func);
			std::cerr << "BulkReduce ";
			break;
	}
	result = r.first;
	rSize = r.second*sizeof(T);
	//for (int j = 0; j < rSize; ++j)
		//std::cerr << (T*) result << " ";
	//std::cerr << "\n ";

}


template <typename T>
void workerFdd<T *>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _apply(func, op, (workerFdd<char> *) dest); break;
		case Int:      _apply(func, op, (workerFdd<int> *) dest); break;
		case LongInt:  _apply(func, op, (workerFdd<long int> *) dest); break;
		case Float:    _apply(func, op, (workerFdd<float> *) dest); break;
		case Double:   _apply(func, op, (workerFdd<double> *) dest); break;
		case CharP:    _applyP(func, op, (workerFdd<char *> *) dest); break;
		case IntP:     _applyP(func, op, (workerFdd<int *> *) dest); break;
		case LongIntP: _applyP(func, op, (workerFdd<long int *> *) dest); break;
		case FloatP:   _applyP(func, op, (workerFdd<float *> *) dest); break;
		case DoubleP:  _applyP(func, op, (workerFdd<double *> *) dest); break;
		case String:   _apply(func, op, (workerFdd<std::string> *) dest); break;
		//case Custom:   _apply(func, op, (workerFdd<void *> *) dest); break;
		case CharV:     _apply(func, op, (workerFdd<std::vector<char>> *) dest); break;
		case IntV:      _apply(func, op, (workerFdd<std::vector<int>> *) dest); break;
		case LongIntV:  _apply(func, op, (workerFdd<std::vector<long int>> *) dest); break;
		case FloatV:    _apply(func, op, (workerFdd<std::vector<float>> *) dest); break;
		case DoubleV:   _apply(func, op, (workerFdd<std::vector<double>> *) dest); break;
	}
}

template <typename T>
template <typename L>
void workerFdd<T *>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _applyI(func, op,  (workerIFdd<L, char> *) dest); break;
		case Int:      _applyI(func, op,  (workerIFdd<L, int> *) dest); break;
		case LongInt:  _applyI(func, op,  (workerIFdd<L, long int> *) dest); break;
		case Float:    _applyI(func, op,  (workerIFdd<L, float> *) dest); break;
		case Double:   _applyI(func, op,  (workerIFdd<L, double> *) dest); break;
		case CharP:    _applyIP(func, op, (workerIFdd<L, char *> *) dest); break;
		case IntP:     _applyIP(func, op, (workerIFdd<L, int *> *) dest); break;
		case LongIntP: _applyIP(func, op, (workerIFdd<L, long int *> *) dest); break;
		case FloatP:   _applyIP(func, op, (workerIFdd<L, float *> *) dest); break;
		case DoubleP:  _applyIP(func, op, (workerIFdd<L, double *> *) dest); break;
		case String:   _applyI(func, op,  (workerIFdd<L, std::string> *) dest); break;
		//case Custom:   _applyI(func, op, (workerFdd<L, void *> *) dest); break;
		case CharV:     _applyI(func, op, (workerIFdd<L, std::vector<char>> *) dest); break;
		case IntV:      _applyI(func, op, (workerIFdd<L, std::vector<int>> *) dest); break;
		case LongIntV:  _applyI(func, op, (workerIFdd<L, std::vector<long int>> *) dest); break;
		case FloatV:    _applyI(func, op, (workerIFdd<L, std::vector<float>> *) dest); break;
		case DoubleV:   _applyI(func, op, (workerIFdd<L, std::vector<double>> *) dest); break;
	}
}

// -------------------------- Public Functions ------------------------ //

template <typename T>
void workerFdd<T *>::apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize){ 
	if (op & OP_GENERICREDUCE){
		_applyReduce(func, op, result, rSize);
	}else{
		switch (dest->getKeyType()){
			case Null:     _preApply(func, op, dest);break;
			case Char:     _preApplyI<char>(func, op, dest); break;
			case Int:      _preApplyI<int>(func, op, dest); break;
			case LongInt:  _preApplyI<long int>(func, op, dest); break;
			case Float:    _preApplyI<float>(func, op, dest); break;
			case Double:   _preApplyI<double>(func, op, dest); break;
			case String:   _preApplyI<std::string>(func, op, dest); break;
		}
	}
}

template <typename T>
void workerFdd<T*>::setData(T ** data, size_t *lineSizes, size_t size) {
	this->localData->setData( data, lineSizes, size);
}

template <typename T>
void workerFdd<T*>::setDataRaw(void * data, size_t *lineSizes, size_t size) {
	this->localData->setDataRaw( data, lineSizes, size);
}


template <typename T>
size_t * workerFdd<T*>::getLineSizes(){ 
	return this->localData->getLineSizes(); 
}

template <typename T>
void workerFdd<T*>::insert(T* & in, size_t s){ 
	this->localData->insert(in, s); 
}

template <typename T>
void workerFdd<T*>::insert(std::list< std::pair<T*, size_t> > & in){ 
	typename std::list< std::pair<T*, size_t> >::iterator it;

	if (this->localData->getSize() < in.size())
		this->localData->grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		this->localData->insert(it->first, it->second); 
}

template <typename T>
void workerFdd<T*>::collect(fastComm * comm) {
	comm->sendFDDDataCollect(this->id, this->localData->getData(), this->localData->getLineSizes(), this->localData->getSize());
};

extern template class workerFdd<char>;
extern template class workerFdd<int>;
extern template class workerFdd<long int>;
extern template class workerFdd<float>;
extern template class workerFdd<double>;

extern template class workerFdd<std::string>;

template class workerFdd<char *>;
template class workerFdd<int *>;
template class workerFdd<long int *>;
template class workerFdd<float *>;
template class workerFdd<double *>;
//template class workerFdd<void *>;
 
extern template class workerFdd<std::vector<char>>;
extern template class workerFdd<std::vector<int>>;
extern template class workerFdd<std::vector<long int>>;
extern template class workerFdd<std::vector<float>>;
extern template class workerFdd<std::vector<double>>;
