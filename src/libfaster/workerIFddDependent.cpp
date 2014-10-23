#include <list>
#include <iostream>


#include "_workerIFdd.h"
#include "indexedFddStorageExtern.cpp"


// --------- MAP
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::map (workerFddBase * dest, ImapIFunctionP<K,T,L,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		std::pair<L,U> r = mapFunc(ik[i], d[i]);
		ok[i] = r.first;
		od[i] = r.second;
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::map (workerFddBase * dest, IPmapIFunctionP<K,T,L,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	size_t * ols = dest->getLineSizes() ;
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		//mapFunc(ok[i], od[i], ols[i], d[i]);
		std::tuple <L,U,size_t> r = mapFunc(ik[i],  d[i]);
		ok[i] = std::get<0>(r);
		od[i] = std::get<1>(r);
		ols[i] = std::get<2>(r);
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template < typename U>
void faster::_workerIFdd<K,T>::map (workerFddBase * dest, mapIFunctionP<K,T,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		od[i] = mapFunc(ik[i], d[i]);
	}
	//std::cerr << "END ";
}		

template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::map (workerFddBase * dest, PmapIFunctionP<K,T,U> mapFunc){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	size_t * ols = dest->getLineSizes() ;
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";

	#pragma omp parallel for 
	for (size_t i = 0; i < s; ++i){
		//mapFunc(od[i], ols[i], d[i]);
		std::pair<U, size_t> r = mapFunc(ik[i], d[i]);
		od[i] = r.first;
		ols[i] = r.second;
	}
	//std::cerr << "END ";
}		


// --------- MAPBYKEY
/*template <typename K, typename T>
std::vector< std::tuple<K, T*, size_t>> findKeyInterval(K * keys, T * data, size_t size, size_t numKeys){
	std::vector<std::tuple<K, T*, size_t>> keyLocations;
	K * lastKey = &keys[0];
	size_t kCount;
	size_t pos = 0;

	keyLocations.reserve(numKeys);
	keyLocations.insert(keyLocations.end(), std::make_tuple(keys[pos], &data[pos], size));
	kCount = 1;

	if(numKeys == 1) 
		return keyLocations;

	for ( size_t i = 1; i < size; ++i){
		if (*lastKey == keys[i]){
			kCount++;
			continue;
		}else{
			std::get<2>(keyLocations[pos]) = kCount;
			pos ++;
			keyLocations.insert(keyLocations.end(), std::make_tuple(keys[i], &data[i],1));
			kCount = 1;
		}
		lastKey = &keys[i];
	}
	std::get<2>(keyLocations[pos]) = kCount;

	return keyLocations;
}// */
/*template <typename K, typename T>
const std::unordered_map< K, std::list<T*>> findKeyInterval(std::vector<K> & ukeys, K * keys, T * data, size_t fddSize){
	std::unordered_map<K, std::list<T*>> keyLocations;
	std::unordered_map<K, omp_lock_t> locks;

	std::cerr << "FindKeyInterval ";

	// If not groupedByKey, find present keys
	if ( (fddSize > 0) && (ukeys.size() == 0) ){
		std::cerr << "FindUKeys ";
		std::unordered_map<K, bool> keyMap;
		
		keyMap.reserve(fddSize);
		
		//std::cerr << "\n";
		for ( size_t i = 0; i < fddSize; ++i){
			keyMap[keys[i]] = true;
			//std::cerr << keys[i] << " ";
		}
		//std::cerr << "\n";
		
		ukeys.resize(keyMap.size());

		size_t i = 0;
		for(auto it = keyMap.begin(); it != keyMap.end(); it++){
			ukeys[i++] = it->first;
			//std::cerr << it->first << " ";
		}
		//std::cerr << "\n";
	}

	std::cerr << "Init ";
	locks.reserve(ukeys.size()*1.1);
	keyLocations.reserve(ukeys.size()*1.1);

	for ( size_t i = 0; i < ukeys.size(); ++i){
		K key = ukeys[i];
		keyLocations[key] = std::list<T*>();
		locks[key] = {};
	}

	std::cerr << "FindKeyData ";
	//#pragma omp parallel
	{
		//#pragma omp for
		for ( size_t i = 0; i < ukeys.size(); ++i){
			auto lock = locks.find(ukeys[i]);
			omp_init_lock( &lock->second );

		}

		//#pragma omp for
		for ( size_t i = 0; i < fddSize; ++i){
			K key = keys[i];
			T * d = &data[i];
			auto lock = locks.find(key);
			auto l = keyLocations.find(key);

			//if ( l == keyLocations.end() ) continue;
			if ( l == keyLocations.end() ) { std::cerr << "ERROR: INCINSISTENCIES IN THE CODE!!!"; exit(243);};
			if ( lock == locks.end() ) continue;

			omp_set_lock(&lock->second);

			l->second.push_back(d);

			omp_unset_lock(&lock->second);
		}

		//#pragma omp for
		for ( size_t i = 0; i < ukeys.size(); ++i){
			auto lock = locks.find(ukeys[i]);
			omp_destroy_lock( &lock->second );
		}
	}

	return keyLocations;
} // */
template <typename K, typename T>
std::vector< std::list<T*>* > findKeyInterval(std::vector<K> & ukeys, K * keys, T * data, size_t fddSize){
	std::unordered_map<K, std::list<T*>*> keyLocations;
	bool generateUK = (ukeys.size() == 0) ;
	size_t numKeys = 0;

	if (generateUK){
		ukeys.reserve(fddSize);
	}

	keyLocations.reserve( fddSize );

	for ( size_t i = 0; i < fddSize; i++){
		K & key = keys[i];
		T * d = &data[i];
		auto l = keyLocations.find(key);

		if ( l == keyLocations.end() ) {
			auto dl = new std::list<T*>();

			dl->push_back(d);
			keyLocations[key] = dl;

			if (generateUK){
				ukeys.insert(ukeys.end(), key);
			}
		}else{
			l->second->push_back(d);
		}
	}

	if (generateUK){
		ukeys.shrink_to_fit();
	}


	std::vector< std::list<T*>* > keyLocationsV(ukeys.size(), NULL);

	for ( size_t i = 0; i < ukeys.size(); i++ ){
		K & key = ukeys[i];
		auto l = keyLocations.find(key);

		if ( l == keyLocations.end() ){
			keyLocationsV[i] = new std::list<T*>();
		}else{
			keyLocationsV[i] = l->second;
			l->second = NULL;
		}
	}
	
	// TODO - MAKE THIS RIGHT!!! these keys should be included
	for ( auto it = keyLocations.begin() ; it != keyLocations.end(); it++ ){
		if ( it->second != NULL ){
			delete it->second;
		}
	}

	return keyLocationsV;
}

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::mapByKey (workerFddBase * dest, ImapByKeyIFunctionP<K,T,L,U> mapByKeyFunc){

	K * ik = (K*) this->localData->getKeys();
	T * id = this->localData->getData();
	size_t s = this->localData->getSize();

	auto keyLocations = findKeyInterval(this->uKeys, ik, id, s);
	//dest->setSize(this->uKeys.size());
	dest->setSize(this->uKeys.size());
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();

	#pragma omp parallel for
	for (size_t i = 0; i < this->uKeys.size(); i++){
		std::pair<L,U> r = mapByKeyFunc(this->uKeys[i], keyLocations[i]);

		ok[i] = r.first;
		od[i] = r.second;

		delete keyLocations[i];
	}

}		

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::mapByKey (workerFddBase * dest, IPmapByKeyIFunctionP<K,T,L,U> mapByKeyFunc){

	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	size_t * ols = dest->getLineSizes() ;

	auto keyLocations = findKeyInterval(this->uKeys, ik, d, s);
	//dest->setSize(this->uKeys.size());
	dest->setSize(keyLocations.size());
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();


	#pragma omp parallel for 
	for (size_t i = 0; i < this->uKeys.size(); ++i){
		//K & key = this->uKeys[i];
		//auto loc = keyLocations.find(key);
		K & key = this->uKeys[i];
		auto loc = keyLocations[i];

		std::tuple <L,U,size_t> r = mapByKeyFunc(key, loc);

		ok[i] = std::get<0>(r);
		od[i] = std::get<1>(r);
		ols[i] = std::get<2>(r);

		delete loc;
	}
}		

template <typename K, typename T>
template < typename U>
void faster::_workerIFdd<K,T>::mapByKey (workerFddBase * dest, mapByKeyIFunctionP<K,T,U> mapByKeyFunc){

	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();

	auto keyLocations = findKeyInterval(this->uKeys, ik, d, s);
	//dest->setSize(this->uKeys.size());
	dest->setSize(keyLocations.size());
	U * od = (U*) dest->getData();


	#pragma omp parallel for 
	for (size_t i = 0; i < this->uKeys.size(); ++i){
		//K & key = this->uKeys[i];
		//auto loc = keyLocations.find(key);
		K & key = this->uKeys[i];
		auto loc = keyLocations[i];

		od[i] = mapByKeyFunc(key, loc);

		delete loc;
	}
}		

template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::mapByKey (workerFddBase * dest, PmapByKeyIFunctionP<K,T,U> mapByKeyFunc){

	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();

	auto keyLocations = findKeyInterval(this->uKeys, ik, d, s);
	//dest->setSize(this->uKeys.size());
	dest->setSize(keyLocations.size());
	size_t * ols = dest->getLineSizes() ;
	U * od = (U*) dest->getData();

	//std::cerr << "START " << id << " " << s << "  ";


	#pragma omp parallel for 
	for (size_t i = 0; i < this->uKeys.size(); ++i){
		//K & key = this->uKeys[i];
		//auto loc = keyLocations.find(key);
		K & key = this->uKeys[i];
		auto loc = keyLocations[i];

		std::pair<U, size_t> r = mapByKeyFunc(key, loc);

		od[i] = r.first;
		ols[i] = r.second;

		delete loc;
	}
	//std::cerr << "END ";
}		



// --------- BulkMAP
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::bulkMap (workerFddBase * dest, IbulkMapIFunctionP<K,T,L,U> bulkMapFunc){
	bulkMapFunc((L*) (dest->getKeys()), (U *) dest->getData(), (K*) this->localData->getKeys(), (T *)this->localData->getData(), this->localData->getSize());
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::bulkMap (workerFddBase * dest, IPbulkMapIFunctionP<K,T,L,U> bulkMapFunc){
	bulkMapFunc((L*) (dest->getKeys()), (U*) dest->getData(), dest->getLineSizes(), (K*) this->localData->getKeys(), (T *) this->localData->getData(), this->localData->getSize());
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::bulkMap (workerFddBase * dest, bulkMapIFunctionP<K,T,U> bulkMapFunc){
	bulkMapFunc((U*) dest->getData(), (K*) this->localData->getKeys(), (T *)this->localData->getData(), this->localData->getSize());
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::bulkMap (workerFddBase * dest, PbulkMapIFunctionP<K,T,U> bulkMapFunc){
	bulkMapFunc((U*) dest->getData(), dest->getLineSizes(), (K*) this->localData->getKeys(), (T *) this->localData->getData(), this->localData->getSize());
}


// --------- FlatMAP

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::flatMap(workerFddBase * dest,  IflatMapIFunctionP<K,T,L,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	std::list<std::pair<L,U>> resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<L,U>> partResultList;

		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::list<std::pair<L,U>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::flatMap(workerFddBase * dest,  IPflatMapIFunctionP<K,T,L,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	std::list< std::tuple<L, U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::tuple<L, U, size_t>> partResultList;

		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::list<std::tuple<L, U, size_t>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::flatMap(workerFddBase * dest,  flatMapIFunctionP<K,T,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	std::list<U> resultList;

	#pragma omp parallel 
	{
		std::list<U> partResultList;

		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::list<U> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest->insertl(&resultList);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::flatMap(workerFddBase * dest,  PflatMapIFunctionP<K,T,U> flatMapFunc ){
	T * d = this->localData->getData();
	size_t s = this->localData->getSize();
	K * ik = (K*) this->localData->getKeys();
	std::list< std::pair<U, size_t> > resultList;

	#pragma omp parallel 
	{
		std::list<std::pair<U, size_t>> partResultList;

		#pragma omp for 
		for (size_t i = 0; i < s; ++i){
			std::list<std::pair<U, size_t>> r = flatMapFunc(ik[i], d[i]);

			partResultList.insert(partResultList.end(), r.begin(), r.end());
		}

		#pragma omp critical
		resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

	}
	dest->insertl(&resultList);
}

template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::bulkFlatMap(workerFddBase * dest,  IbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t rSize;

	bulkFlatMapFunc(ok, result, rSize, this->localData->getKeys(), this->localData->getData(), this->localData->getSize());
	dest->setData(ok, result, rSize);
}
template <typename K, typename T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc ){
	L * ok;
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc(ok, result, rDataSizes, rSize, this->localData->getKeys(), (T*) this->localData->getData(), this->localData->getSize());
	dest->setData( ok, result, rDataSizes, rSize);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::bulkFlatMap(workerFddBase * dest,  bulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t rSize;

	bulkFlatMapFunc(result, rSize, this->localData->getKeys(), this->localData->getData(), this->localData->getSize());
	dest->setData(result, rSize);
}
template <typename K, typename T>
template <typename U>
void faster::_workerIFdd<K,T>::bulkFlatMap(workerFddBase * dest,  PbulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc ){
	U * result;
	size_t * rDataSizes = NULL;
	size_t rSize;

	bulkFlatMapFunc( result, rDataSizes, rSize, this->localData->getKeys(), (T*) this->localData->getData(), this->localData->getSize());
	dest->setData( result, rDataSizes, rSize);
}



// Not Pointer -> Not Pointer
template <class K, class T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::_applyI(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<L,U>(dest, (ImapIFunctionP<K,T,L,U>) func);
			//std::cerr << "Map ";
			break;
		case OP_MapByKey:
			mapByKey<L,U>(dest, (ImapByKeyIFunctionP<K,T,L,U>) func);
			//std::cerr << "MapByKey ";
			break;
		case OP_BulkMap:
			bulkMap<L,U>(dest, ( IbulkMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<L,U>(dest, ( IflatMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<L,U>(dest, ( IbulkFlatMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <class K, class T>
template <typename L, typename U>
void faster::_workerIFdd<K,T>::_applyIP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<L,U>(dest, (IPmapIFunctionP<K,T,L,U>) func);
			//std::cerr << "Map ";
			break;
		case OP_MapByKey:
			mapByKey<L,U>(dest, (IPmapByKeyIFunctionP<K,T,L,U>) func);
			//std::cerr << "MapByKey ";
			break;
		case OP_BulkMap:
			bulkMap<L,U>(dest, ( IPbulkMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<L,U>(dest, ( IPflatMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<L,U>(dest, ( IPbulkFlatMapIFunctionP<K,T,L,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}
// Not Pointer -> Not Pointer
template <class K, class T>
template <typename U>
void faster::_workerIFdd<K,T>::_apply(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<U>(dest, (mapIFunctionP<K,T,U>) func);
			//std::cerr << "Map ";
			break;
		case OP_MapByKey:
			mapByKey<U>(dest, (mapByKeyIFunctionP<K,T,U>) func);
			//std::cerr << "MapByKey ";
			break;
		case OP_BulkMap:
			bulkMap<U>(dest, ( bulkMapIFunctionP<K,T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<U>(dest, ( flatMapIFunctionP<K,T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<U>(dest, ( bulkFlatMapIFunctionP<K,T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}

// Not Pointer -> Pointer
template <class K, class T>
template <typename U>
void faster::_workerIFdd<K,T>::_applyP(void * func, fddOpType op, workerFddBase * dest){
	switch (op){
		case OP_Map:
			map<U>(dest, (PmapIFunctionP<K,T,U>) func);
			//std::cerr << "Map ";
			break;
		case OP_MapByKey:
			mapByKey<U>(dest, (PmapByKeyIFunctionP<K,T,U>) func);
			//std::cerr << "MapByKey ";
			break;
		case OP_BulkMap:
			bulkMap<U>(dest, ( PbulkMapIFunctionP<K,T,U> ) func);
			//std::cerr << "BulkMap ";
			break;
		case OP_FlatMap:
			flatMap<U>(dest, ( PflatMapIFunctionP<K,T,U> ) func);
			//std::cerr << "FlatMap ";
			break;
		case OP_BulkFlatMap:
			bulkFlatMap<U>(dest, ( PbulkFlatMapIFunctionP<K,T,U> ) func);
			//std::cerr << "BulkFlatMap ";
			break;
	}
}


template <class K, class T>
template <typename L>
void faster::_workerIFdd<K,T>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: break;
		case Char:      _applyI<L,char>	(func, op, dest); break;
		case Int:       _applyI<L,int> 	(func, op,  dest); break;
		case LongInt:   _applyI<L,long int>(func, op,  dest); break;
		case Float:     _applyI<L,float>   (func, op,  dest); break;
		case Double:    _applyI<L,double>  (func, op,  dest); break;
		case CharP:    _applyIP<L,char *>  (func, op, dest); break;
		case IntP:     _applyIP<L,int *>   (func, op, dest); break;
		case LongIntP: _applyIP<L,long int *> (func, op, dest); break;
		case FloatP:   _applyIP<L,float *>    (func, op, dest); break;
		case DoubleP:  _applyIP<L,double *>   (func, op, dest); break;
		case String:   _applyI<L,std::string>(func, op,  dest); break;
		//case Custom:  _applyI(func, op, dest); break;
		case CharV:    _applyI<L,std::vector<char>>    (func, op, dest); break;
		case IntV:     _applyI<L,std::vector<int>>     (func, op, dest); break;
		case LongIntV: _applyI<L,std::vector<long int>>(func, op, dest); break;
		case FloatV:   _applyI<L,std::vector<float>>   (func, op, dest); break;
		case DoubleV:  _applyI<L,std::vector<double>>  (func, op, dest); break;
	}

}

template <class K, class T>
void faster::_workerIFdd<K,T>::_preApply(void * func, fddOpType op, workerFddBase * dest){
	switch (dest->getType()){
		case Null: break;
		case Char:      _apply<char> 	(func, op, dest); break;
		case Int:       _apply<int> 	(func, op, dest); break;
		case LongInt:   _apply<long int> (func, op, dest); break;
		case Float:     _apply<float> 	(func, op, dest); break;
		case Double:    _apply<double> 	(func, op, dest); break;
		case CharP:    _applyP<char *> 	(func, op, dest); break;
		case IntP:     _applyP<int *> 	(func, op, dest); break;
		case LongIntP: _applyP<long int *> (func, op, dest); break;
		case FloatP:   _applyP<float *> (func, op, dest); break;
		case DoubleP:  _applyP<double *> (func, op, dest); break;
		case String:    _apply<std::string>(func, op, dest); break;
		//case Custom:  _apply<void *>(func, op, dest); break;
		case CharV:   _apply<std::vector<char>>  (func, op,  dest); break;
		case IntV:    _apply<std::vector<int>> 	 (func, op,  dest); break;
		case LongIntV:_apply<std::vector<long int>>(func, op,  dest); break;
		case FloatV:  _apply<std::vector<float>>  (func, op,  dest); break;
		case DoubleV: _apply<std::vector<double>> (func, op,  dest); break;
	}

}
template <typename K, typename T>
void faster::_workerIFdd<K,T>::applyDependent(void * func, fddOpType op, workerFddBase * dest){ 
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



