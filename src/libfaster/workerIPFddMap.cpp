#include <iostream>
#include <tuple>

#include "workerIFdd.h"
#include "workerFdd.h"
#include "indexedFddStorage.h"

// MAP
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::map (workerIFdd<L,U> & dest, ImapIPFunctionP<K,T,L,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	K * ik = localData->getKeys();
	L * ok = dest.getKeys();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(ik[i], d[i], ls[i]);
		ok[i] = r.first;
		dest[i] = r.second;
	}
	//std::cerr << "END ";
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::map (workerIFdd<L,U> & dest, IPmapIPFunctionP<K,T,L,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	K * ik = localData->getKeys();
	L * ok = dest.getKeys();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < localData->getSize(); ++i){
		std::tuple<L,U,size_t> r = mapFunc(ik[i], d[i], ls[i]);
		ok[i] = std::get<0>(r);
		dest[i] = std::get<1>(r);
		dls[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::map (workerFdd<U> & dest, mapIPFunctionP<K,T,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	size_t * ls = localData->getLineSizes();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < s; ++i){
		dest[i] = mapFunc(ik[i], d[i], ls[i]);
	}
	//std::cerr << "END ";
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::map (workerFdd<U> & dest, PmapIPFunctionP<K,T,U> mapFunc){
	T ** d = localData->getData();
	size_t s = localData->getSize();
	K * ik = localData->getKeys();
	size_t * ls = localData->getLineSizes();
	size_t * dls = dest.getLineSizes();
	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (int i = 0; i < localData->getSize(); ++i){
		std::pair<U,size_t> r = mapFunc(ik[i], d[i], ls[i]);
		dest[i] = r.first;
		dls[i] = r.second;
	}
	//std::cerr << "END ";
}		




template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::bulkMap (workerIFdd<L,U> & dest, IbulkMapIPFunctionP<K,T,L,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	K * ik = localData->getKeys();
	L * ok = dest.getKeys();

	bulkMapFunc(ok, (U*) dest.getData(), ik, (T **)localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::bulkMap (workerIFdd<L,U> & dest, IPbulkMapIPFunctionP<K,T,L,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	K * ik = localData->getKeys();
	L * ok = dest.getKeys();

	bulkMapFunc(ok, (U*) dest.getData(), dest.getLineSizes(), ik, (T **)localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::bulkMap (workerFdd<U> & dest, bulkMapIPFunctionP<K,T,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	K * ik = localData->getKeys();

	bulkMapFunc((U*) dest.getData(), ik, (T **)localData->getData(), ls, s);
}
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::bulkMap (workerFdd<U> & dest, PbulkMapIPFunctionP<K,T,U> bulkMapFunc){
	size_t s = localData->getSize();
	size_t * ls = localData->getLineSizes();
	K * ik = localData->getKeys();

	bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), ik, (T **)localData->getData(), ls, s);
}


// Pointer -> Not Pointer
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::_applyIMap(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (ImapIPFunctionP<K,T,L,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IbulkMapIPFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
	}
}
// Pointer -> Pointer
template <typename K, typename T>
template <typename L, typename U>
void workerIFdd<K,T*>::_applyIPMap(void * func, fddOpType op, workerIFdd<L, U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (IPmapIPFunctionP<K,T,L,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( IPbulkMapIPFunctionP<K,T,L,U> ) func);
			std::cerr << "BulkMap ";
			break;
	}
}

template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::_applyMap(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (mapIPFunctionP<K,T,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( bulkMapIPFunctionP<K,T,U> ) func);
			std::cerr << "BulkMap ";
			break;
	}
}

// Pointer -> Pointer
template <typename K, typename T>
template <typename U>
void workerIFdd<K,T*>::_applyPMap(void * func, fddOpType op, workerFdd<U> * dest){
	switch (op){
		case OP_Map:
			map(*dest, (PmapIPFunctionP<K,T,U>) func);
			std::cerr << "Map";
			break;
		case OP_BulkMap:
			bulkMap(*dest, ( PbulkMapIPFunctionP<K,T,U> ) func);
			std::cerr << "BulkMap ";
			break;
	}
}

template <typename K, typename T>
template <typename L>
void workerIFdd<K,T*>::_preApplyIMap(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: 	break;
		case Char:     _applyIMap(func, op,  (workerIFdd<L,char> *) dest); break;
		case Int:      _applyIMap(func, op,  (workerIFdd<L,int> *) dest); break;
		case LongInt:  _applyIMap(func, op,  (workerIFdd<L,long int> *) dest); break;
		case Float:    _applyIMap(func, op,  (workerIFdd<L,float> *) dest); break;
		case Double:   _applyIMap(func, op,  (workerIFdd<L,double> *) dest); break;
		case CharP:    _applyIPMap(func, op, (workerIFdd<L,char *> *) dest); break;
		case IntP:     _applyIPMap(func, op, (workerIFdd<L,int *> *) dest); break;
		case LongIntP: _applyIPMap(func, op, (workerIFdd<L,long int *> *) dest); break;
		case FloatP:   _applyIPMap(func, op, (workerIFdd<L,float *> *) dest); break;
		case DoubleP:  _applyIPMap(func, op, (workerIFdd<L,double *> *) dest); break;
		case String:   _applyIMap(func, op,  (workerIFdd<L,std::string> *) dest); break;
		//case Custom:  _applyIMap(func, op, (workerIFdd<L,void *> *) dest); break;
		//case CharV:   _applyIMap(func, op, (workerIFdd<L,std::vector<char>> *) dest); break;
		//case IntV:    _applyIMap(func, op, (workerIFdd<L,std::vector<int>> *) dest); break;
		//case LongIntV:_applyIMap(func, op, (workerIFdd<L,std::vector<long int>> *) dest); break;
		//case FloatV:  _applyIMap(func, op, (workerIFdd<L,std::vector<float>> *) dest); break;
		//case DoubleV: _applyIMap(func, op, (workerIFdd<L,std::vector<double>> *) dest); break;
	}

}

template <typename K, typename T>
void workerIFdd<K,T*>::_preApplyMap(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: 	break;
		case Char:     _applyMap(func, op,  (workerFdd<char> *) dest); break;
		case Int:      _applyMap(func, op,  (workerFdd<int> *) dest); break;
		case LongInt:  _applyMap(func, op,  (workerFdd<long int> *) dest); break;
		case Float:    _applyMap(func, op,  (workerFdd<float> *) dest); break;
		case Double:   _applyMap(func, op,  (workerFdd<double> *) dest); break;
		case CharP:    _applyPMap(func, op, (workerFdd<char *> *) dest); break;
		case IntP:     _applyPMap(func, op, (workerFdd<int *> *) dest); break;
		case LongIntP: _applyPMap(func, op, (workerFdd<long int *> *) dest); break;
		case FloatP:   _applyPMap(func, op, (workerFdd<float *> *) dest); break;
		case DoubleP:  _applyPMap(func, op, (workerFdd<double *> *) dest); break;
		case String:   _applyMap(func, op,  (workerFdd<std::string> *) dest); break;
		//case Custom:  _applyMap(func, op,  (workerFdd<void *> *) dest); break;
		//case CharV:   _applyMap(func, op,  (workerFdd<std::vector<char>> *) dest); break;
		//case IntV:    _applyMap(func, op,  (workerFdd<std::vector<int>> *) dest); break;
		//case LongIntV:_applyMap(func, op,  (workerFdd<std::vector<long int>> *) dest); break;
		//case FloatV:  _applyMap(func, op,  (workerFdd<std::vector<float>> *) dest); break;
		//case DoubleV: _applyMap(func, op,  (workerFdd<std::vector<double>> *) dest); break;
	}

}

template <typename K, typename T>
void workerIFdd<K,T*>::applyMap(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getKeyType()){
		case Null:     _preApplyMap(func, op, dest);break;
		case Char:     _preApplyIMap<char>(func, op, dest); break;
		case Int:      _preApplyIMap<int>(func, op, dest); break;
		case LongInt:  _preApplyIMap<long int>(func, op, dest); break;
		case Float:    _preApplyIMap<float>(func, op, dest); break;
		case Double:   _preApplyIMap<double>(func, op, dest); break;
		case String:   _preApplyIMap<std::string>(func, op, dest); break;
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

