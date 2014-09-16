#include <chrono>
#include <omp.h>
#include <algorithm>

#include "workerFddGroup.h"
#include "workerFdd.h"
#include "fastComm.h"
#include "indexedFddStorageExtern.cpp"

template <typename K>
faster::workerFddGroup<K>::workerFddGroup(unsigned long int id, fddType keyT, std::vector< workerFddBase* > & members): workerFddBase(id, Null){
	keyType = keyT;
	this->members = members;
}


template <typename K>
std::unordered_map<K, std::pair<void*, size_t>> findKeyInterval(std::vector<K> & ukeys, faster::workerFddBase * wfdd){
	std::unordered_map<K, std::pair<void*, size_t>> keyLocations;
	size_t kCount;
	size_t pos = 0;
	char * data = (char*) wfdd->getData();
	K * keys = (K *) wfdd->getKeys();
	size_t fddSize = wfdd->getSize();
	K * lastKey = &keys[0];
	int baseSize = wfdd->baseSize();

	//std::cerr << "    findKeyInterval " << fddSize << "\n";

	keyLocations.reserve(ukeys.size());
	keyLocations[ukeys[pos]] = std::make_pair( &data[0], 1 );
	kCount = 1;

	//for ( int i = 0; i < ukeys.size(); ++i){
		//std::cerr << ukeys[i] << " ";
	//}
	//std::cerr << "\n";

	if(ukeys.size() == 1) 
		return keyLocations;
	

	//std::cerr << "0 \033[0;31m0\033[0m";
	//std::cerr << keys[0] << " ";
	for ( size_t i = 1; i < fddSize; ++i){
		if (*lastKey == keys[i]){
			kCount++;
		}else{
			//std::cerr << "(" << kCount << ") ";
			keyLocations[ukeys[pos]].second = kCount;
			// Find next key
			while(ukeys[pos] != keys[i]){
				if (pos < ukeys.size()){
					//TODO INSERT NOT PRESENT ITENS...
					//std::cerr << "(S" << ukeys[pos]<< ") ";
					pos ++;
				}else
					return keyLocations;
			}
			//std::cerr << "(I" << ukeys[pos]<< ") ";
			keyLocations[ukeys[pos]] = std::make_pair(&data[baseSize*i], 1);
			kCount = 1;
			lastKey = &keys[i];
		}
		//std::cerr << i << " \033[0;31m" << keys[i] << "\033[0m ";
	}
	//std::cerr << "(" << kCount << ") \n";
	keyLocations[ukeys[pos]].second = kCount;
	//std::cerr << "(" << ukeys[pos] << "=" << kCount << ")\n";
	//std::cerr << "    DONE\n";

	return keyLocations;
}

template <typename K>
void faster::workerFddGroup<K>::updateByKey(void * mapByKeyFunc){

	std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];

	for (int i = 0; i < members.size(); ++i){
		keyLocations[i] = findKeyInterval(uKeys, members[i]);
	}

	if ( members.size() < 3 ){  
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];
			((updateByKeyG2FunctionP<K>) mapByKeyFunc) 
				(key, 
				 location0.first, location0.second, 
				 location1.first, location1.second );
		}
	}else{
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];
			auto location2 = keyLocations[2][key];
			((updateByKeyG3FunctionP<K>) mapByKeyFunc) 
				(key, 
				 location0.first, location0.second, 
				 location1.first, location1.second, 
				 location2.first, location2.second );
		}
	}
}


template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::mapByKey(workerFddBase * dest, void * mapByKeyFunc){
	dest->setSize(uKeys.size());
	U * od = (U*) dest->getData();
	std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	
	//std::cerr << "START " << id << " " << s << "  ";

	for (int i = 0; i < members.size(); ++i){
		keyLocations[i] = findKeyInterval(uKeys, members[i]);
	}

	if ( members.size() < 3 ){  
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];

			od[i] = ((mapByKeyG2FunctionP<K,U>) mapByKeyFunc)
				( key, 
				  location0.first, location0.second, 
				  location1.first, location1.second );
		}
	}else{
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];
			auto location2 = keyLocations[2][key];

			od[i] = ((mapByKeyG3FunctionP<K,U>) mapByKeyFunc)
				( key, 
				  location0.first, location0.second, 
				  location1.first, location1.second, 
				  location2.first, location2.second );
		}
	}
	//std::cerr << "END ";
}		

template <typename K>
//template <typename L, typename U, typename T0, typename T1, typename T2>
template <typename L, typename U>
void faster::workerFddGroup<K>::mapByKeyI(workerFddBase * dest, void * mapByKeyFunc){
	std::pair<L,U> r;
	dest->setSize(uKeys.size());
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();
	std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];

	//std::cerr << "START " << id << " S:" << uKeys.size() << " \n";


	for (int i = 0; i < members.size(); ++i){
		keyLocations[i] = findKeyInterval(uKeys, members[i]);
		//for ( auto it = keyLocations[i].begin(); it != keyLocations[i].end(); it++)
			//std::cerr << it->first << ":" << it->second.second << " ";
		//std::cerr << "\n    \033[0;34m" << i << " " << "\033[0m - ";
		//for (int j = 0; j < members[i]->getSize(); ++j){
			//std::cerr  << ((K*)members[i]->getKeys())[j] << ":" << ((int*) members[i]->getData())[j] << " ";
		//}
		//std::cerr << "\n";
	}

	if ( members.size() <3 ){  
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];
			std::pair<L,U> r;

			r = ( (ImapByKeyG2FunctionP<K,L,U>) mapByKeyFunc) 
				( key, 
				  location0.first, location0.second, 
				  location1.first, location1.second );
		}
	}else{
		#pragma omp parallel for 
		for (int i = 0; i < uKeys.size(); ++i){
			K key = uKeys[i];
			auto location0 = keyLocations[0][key];
			auto location1 = keyLocations[1][key];
			auto location2 = keyLocations[2][key];
			std::pair<L,U> r;

			r = ( (ImapByKeyG3FunctionP<K,L,U>) mapByKeyFunc )
				( key, 
				  location0.first, location0.second, 
				  location1.first, location1.second, 
				  location2.first, location2.second );
			ok[i] = r.first;
			od[i] = r.second;
		}
	}
	//std::cerr << "END ";
}		

template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::flatMapByKey(workerFddBase * dest, void * mapByKeyFunc){
	std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::list<U> resultList;
	
	//std::cerr << "START " << id << " " << s << "  ";

	for (int i = 0; i < members.size(); ++i){
		keyLocations[i] = findKeyInterval(uKeys, members[i]);
	}

	if ( members.size() < 3 ){  
		#pragma omp parallel 
		{
			std::list<U> pResultList;

			#pragma omp for 
			for (int i = 0; i < uKeys.size(); ++i){
				K key = uKeys[i];
				auto location0 = keyLocations[0][key];
				auto location1 = keyLocations[1][key];

				std::list<U> r = ((flatMapByKeyG2FunctionP<K,U>) mapByKeyFunc)
					( key, 
					  location0.first, location0.second, 
					  location1.first, location1.second );

				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());
			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}
	}else{
		#pragma omp parallel 
		{
			std::list<U> pResultList;

			#pragma omp for 
			for (int i = 0; i < uKeys.size(); ++i){
				K key = uKeys[i];
				auto location0 = keyLocations[0][key];
				auto location1 = keyLocations[1][key];
				auto location2 = keyLocations[2][key];

				std::list<U> r = ((flatMapByKeyG3FunctionP<K,U>) mapByKeyFunc)
					( key, 
					  location0.first, location0.second, 
					  location1.first, location1.second, 
					  location2.first, location2.second );

				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());

			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}
	}
	dest->insertl(&resultList);
	//std::cerr << "END ";
}		

template <typename K>
//template <typename L, typename U, typename T0, typename T1, typename T2>
template <typename L, typename U>
void faster::workerFddGroup<K>::flatMapByKeyI(workerFddBase * dest, void * mapByKeyFunc){
	std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::list<std::pair<L,U>> resultList;

	//std::cerr << "START " << id << " S:" << uKeys.size() << " \n";


	for (int i = 0; i < members.size(); ++i){
		keyLocations[i] = findKeyInterval(uKeys, members[i]);
		//for ( auto it = keyLocations[i].begin(); it != keyLocations[i].end(); it++)
			//std::cerr << it->first << "-" << it->second.second << " ";
		//std::cerr << "\n    \033[0;34m" << i << " " << "\033[0m - ";
		//for (int j = 0; j < members[i]->getSize(); ++j){
			//std::cerr  << ((K*)members[i]->getKeys())[j] << ":" << ((int*) members[i]->getData())[j] << " ";
		//}
		//std::cerr << "\n";
	}
	//std::cerr << "\n";

	if ( members.size() <3 ){  
		//#pragma omp parallel 
		{
			std::list<std::pair<L,U>> pResultList;

			#pragma omp  for 
			for (int i = 0; i < uKeys.size(); ++i){
				K key = uKeys[i];
				auto location0 = keyLocations[0][key];
				auto location1 = keyLocations[1][key];
				std::list<std::pair<L,U>> r = ( (IflatMapByKeyG2FunctionP<K,L,U>) mapByKeyFunc) 
					( key, 
					  location0.first, location0.second, 
					  location1.first, location1.second );

				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());
			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}
	}else{
		#pragma omp parallel 
		{
			std::list<std::pair<L,U>> pResultList;

			#pragma omp  for 
			for (int i = 0; i < uKeys.size(); ++i){
				K key = uKeys[i];
				auto location0 = keyLocations[0][key];
				auto location1 = keyLocations[1][key];
				auto location2 = keyLocations[2][key];
				std::list<std::pair<L,U>> r = ( (IflatMapByKeyG3FunctionP<K,L,U>) mapByKeyFunc )
					( key, 
					  location0.first, location0.second, 
					  location1.first, location1.second, 
					  location2.first, location2.second );

				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());
			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}

	}
	dest->insertl(&resultList);
	//std::cerr << "END ";
}		

template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::_apply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (op){
		case OP_MapByKey:
			//mapByKey<U, T0, T1, T2>(dest, func);
			mapByKey<U>(dest, func);
			//std::cerr << "MapByKey ";
			break;
		case OP_FlatMapByKey:
			//mapByKey<U, T0, T1, T2>(dest, func);
			flatMapByKey<U>(dest, func);
			//std::cerr << "MapByKey ";
			break;
	}
}

template <typename K>
//template <typename L, typename U, typename T0, typename T1, typename T2>
template <typename L, typename U>
void faster::workerFddGroup<K>::_applyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (op){
		case OP_MapByKey:
			//mapByKeyI<L,U, T0, T1, T2>(dest, func);
			mapByKeyI<L,U>(dest, func);
			//std::cerr << "MapByKeyI ";
			break;
		case OP_FlatMapByKey:
			//mapByKeyI<L,U, T0, T1, T2>(dest, func);
			flatMapByKeyI<L,U>(dest, func);
			//std::cerr << "MapByKeyI ";
			break;
	}
}

template <typename K>
//template <typename T0, typename T1, typename T2>
void faster::workerFddGroup<K>::_applyReduce(void * func UNUSED, fddOpType op UNUSED, fastCommBuffer & buffer){
	void * r = NULL;
	size_t rSize = 0;

	switch (op){
		case OP_UpdateByKey:
			updateByKey(func);
			//std::cerr << "Update ";
			break;
		/*case OP_Reduce:
			r = reduce( ( reduceGFunctionP<T> ) func);
			std::cerr << "Reduce ";
			break;
		case OP_BulkReduce:
			r = bulkReduce( ( bulkReduceGFunctionP<T> ) func);
			std::cerr << "BulkReduce ";
			break;*/
	}

	if (op & OP_GENERICREDUCE) buffer.write(r,rSize);
}
template <typename K>
void faster::workerFddGroup<K>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Char:      _apply<char>(func, op,  dest); break;
		case Int:       _apply<int>(func, op,  dest); break;
		case LongInt:   _apply<long int>(func, op, dest); break;
		case Float:     _apply<float>(func, op,  dest); break;
		case Double:    _apply<double>(func, op,  dest); break;
		//case CharP:    _applyP<char *>(func, op, dest); break;
		//case IntP:     _applyP<int *>(func, op, dest); break;
		//case LongIntP: _applyP<long int *>(func, op, dest); break;
		//case FloatP:   _applyP<float *>(func, op, dest); break;
		//case DoubleP:  _applyP<double *>(func, op, dest); break;
		//case Custom:   _apply<void *>(func, op, (workerFdd *) dest); break;
		case String:    _apply<std::string>(func, op,  dest); break;
		case CharV:     _apply<std::vector<char>>(func, op, dest); break;
		case IntV:      _apply<std::vector<int>>(func, op, dest); break;
		case LongIntV:  _apply<std::vector<long int>>(func, op, dest); break;
		case FloatV:    _apply<std::vector<float>>(func, op, dest); break;
		case DoubleV:   _apply<std::vector<double>>(func, op, dest); break;
	}
}

template <typename K>
template <typename L>
void faster::workerFddGroup<K>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:      _applyI<L, char> 	(func, op, dest); break;
		case Int:       _applyI<L, int> 	(func, op, dest); break;
		case LongInt:   _applyI<L, long int> 	(func, op, dest); break;
		case Float:     _applyI<L, float> 	(func, op, dest); break;
		case Double:    _applyI<L, double> 	(func, op, dest); break;
		//case CharP:    _applyIP<L, char *> 	(func, op, dest); break;
		//case IntP:     _applyIP<L, int *> 	(func, op, dest); break;
		//case LongIntP: _applyIP<L, long int *> 	(func, op, dest); break;
		//case FloatP:   _applyIP<L, float *> 	(func, op, dest); break;
		//case DoubleP:  _applyIP<L, double *> 	(func, op, dest); break;
		//case Custom:   _applyI<L, void *> 	(func, op, dest); break;
		case String:    _applyI<L, std::string> (func, op, dest); break;
		case CharV:     _applyI<L, std::vector<char>> 	(func, op, dest); break;
		case IntV:      _applyI<L, std::vector<int>> 	(func, op, dest); break;
		case LongIntV:  _applyI<L, std::vector<long int>>(func, op, dest); break;
		case FloatV:    _applyI<L, std::vector<float>> 	(func, op, dest); break;
		case DoubleV:   _applyI<L, std::vector<double>> (func, op, dest); break;
	}
}

/*
template <typename K>
//template <typename T0, typename T1, typename T2>
void faster::workerFddGroup<K>::_preApply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Char:      _apply<char, T0, T1, T2>(func, op,  dest); break;
		case Int:       _apply<int, T0, T1, T2>(func, op,  dest); break;
		case LongInt:   _apply<long int, T0, T1, T2>(func, op, dest); break;
		case Float:     _apply<float, T0, T1, T2>(func, op,  dest); break;
		case Double:    _apply<double, T0, T1, T2>(func, op,  dest); break;
		//case CharP:    _applyP<char *>(func, op, dest); break;
		//case IntP:     _applyP<int *>(func, op, dest); break;
		//case LongIntP: _applyP<long int *>(func, op, dest); break;
		//case FloatP:   _applyP<float *>(func, op, dest); break;
		//case DoubleP:  _applyP<double *>(func, op, dest); break;
		//case Custom:   _apply<void *>(func, op, (workerFdd *) dest); break;
		case String:    _apply<std::string, T0, T1, T2>(func, op,  dest); break;
		case CharV:     _apply<std::vector<char>, T0, T1, T2>(func, op, dest); break;
		case IntV:      _apply<std::vector<int>, T0, T1, T2>(func, op, dest); break;
		case LongIntV:  _apply<std::vector<long int>, T0, T1, T2>(func, op, dest); break;
		case FloatV:    _apply<std::vector<float>, T0, T1, T2>(func, op, dest); break;
		case DoubleV:   _apply<std::vector<double>, T0, T1, T2>(func, op, dest); break;
	}
}
template <typename K>
//template <typename L, typename T0, typename T1, typename T2>
template <typename L>
void faster::workerFddGroup<K>::_preApplyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (dest->getType()){
		case Null: break;
		case Char:      _applyI<L, char, T0, T1, T2> 	(func, op, dest); break;
		case Int:       _applyI<L, int, T0, T1, T2> 	(func, op, dest); break;
		case LongInt:   _applyI<L, long int, T0, T1, T2> 	(func, op, dest); break;
		case Float:     _applyI<L, float, T0, T1, T2> 	(func, op, dest); break;
		case Double:    _applyI<L, double, T0, T1, T2> 	(func, op, dest); break;
		//case CharP:    _applyIP<L, char *> 	(func, op, dest); break;
		//case IntP:     _applyIP<L, int *> 	(func, op, dest); break;
		//case LongIntP: _applyIP<L, long int *> 	(func, op, dest); break;
		//case FloatP:   _applyIP<L, float *> 	(func, op, dest); break;
		//case DoubleP:  _applyIP<L, double *> 	(func, op, dest); break;
		//case Custom:   _applyI<L, void *> 	(func, op, dest); break;
		case String:    _applyI<L, std::string, T0, T1, T2> (func, op, dest); break;
		case CharV:     _applyI<L, std::vector<char>, T0, T1, T2> 	(func, op, dest); break;
		case IntV:      _applyI<L, std::vector<int>, T0, T1, T2> 	(func, op, dest); break;
		case LongIntV:  _applyI<L, std::vector<long int>, T0, T1, T2>(func, op, dest); break;
		case FloatV:    _applyI<L, std::vector<float>, T0, T1, T2> 	(func, op, dest); break;
		case DoubleV:   _applyI<L, std::vector<double>, T0, T1, T2> (func, op, dest); break;
	}
}

template <typename K>
template <typename T0, typename T1, typename T2>
void faster::workerFddGroup<K>::decodeLast(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){ 
	if (op & OP_GENERICMAP){
		switch (dest->getKeyType()){
			case Null:     break;
			case Char:     _preApplyI<char, T0, T1, T2>(func, op, dest); break;
			case Int:      _preApplyI<int, T0, T1, T2>(func, op, dest); break;
			case LongInt:  _preApplyI<long int, T0, T1, T2>(func, op, dest); break;
			case Float:    _preApplyI<float, T0, T1, T2>(func, op, dest); break;
			case Double:   _preApplyI<double, T0, T1, T2>(func, op, dest); break;
			case String:   _preApplyI<std::string, T0, T1, T2>(func, op, dest); break;
		}
	}else{
		_applyReduce<T0, T1, T2>(func, op, buffer);
	}
}

template <typename K>
template <typename T0, typename T1>
inline void faster::workerFddGroup<K>::decodeThird(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){
	if (members.size() < 3){
		switch (members[2]->getType()){
			case Null:     break;
			case Char:     decodeLast<T0, T1, char>(func, op, dest, buffer); break;
			case Int:      decodeLast<T0, T1, int>(func, op, dest, buffer); break;
			case LongInt:  decodeLast<T0, T1, long int>(func, op, dest, buffer); break;
			case Float:    decodeLast<T0, T1, float>(func, op, dest, buffer); break;
			case Double:   decodeLast<T0, T1, double>(func, op, dest, buffer); break;
			case String:   decodeLast<T0, T1, std::string>(func, op, dest, buffer); break;
		}
	}else{
		decodeLast<T0, T1, char>(func, op, dest, buffer);
	}
}


template <typename K>
template <typename T0>
inline void faster::workerFddGroup<K>::decodeSecond(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){
	switch (members[1]->getType()){
		case Null:     break;
		case Char:     decodeThird<T0, char>(func, op, dest, buffer); break;
		case Int:      decodeThird<T0, int>(func, op, dest, buffer); break;
		case LongInt:  decodeThird<T0, long int>(func, op, dest, buffer); break;
		case Float:    decodeThird<T0, float>(func, op, dest, buffer); break;
		case Double:   decodeThird<T0, double>(func, op, dest, buffer); break;
		case String:   decodeThird<T0, std::string>(func, op, dest, buffer); break;
	}
}


template <typename K>
inline void faster::workerFddGroup<K>::apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){
		switch (members[0]->getType()){
			case Null:     break;
			case Char:     decodeSecond<char>(func, op, dest, buffer); break;
			case Int:      decodeSecond<int>(func, op, dest, buffer); break;
			case LongInt:  decodeSecond<long int>(func, op, dest, buffer); break;
			case Float:    decodeSecond<float>(func, op, dest, buffer); break;
			case Double:   decodeSecond<double>(func, op, dest, buffer); break;
			case String:   decodeSecond<std::string>(func, op, dest, buffer); break;
		}
}// */

template <typename K>
inline void faster::workerFddGroup<K>::apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer){
	if (op & OP_GENERICMAP){
		switch (dest->getKeyType()){
			case Null:     _preApply(func, op, dest); break;
			case Char:     _preApplyI<char>(func, op, dest); break;
			case Int:      _preApplyI<int>(func, op, dest); break;
			case LongInt:  _preApplyI<long int>(func, op, dest); break;
			case Float:    _preApplyI<float>(func, op, dest); break;
			case Double:   _preApplyI<double>(func, op, dest); break;
			case String:   _preApplyI<std::string>(func, op, dest); break;
		}
	}else{
		_applyReduce(func, op, buffer);
	}
}

template <typename K>
void faster::workerFddGroup<K>::cogroup(fastComm *comm){
	//std::cerr << "\n    Cogroup\n";
	unsigned long tid = 0;

	//std::cerr << "      RecvKeyMap\n";
	comm->recvKeyMap(tid, keyMap);

	uKeys.reserve(keyMap.size());

	//std::cerr << "        My Keys: ";
	for( auto it = keyMap.begin(); it != keyMap.end(); it++ ){
		if ( it->second == comm->getProcId() ){
			uKeys.insert(uKeys.end(), it->first);
			//std::cerr << it->first << " ";
		}
	}
	//std::cerr << "\n";

	uKeys.shrink_to_fit();

	std::sort(uKeys.begin(), uKeys.end());

	for ( int i = 1; i < members.size(); ++i){
		members[i]->exchangeDataByKey(comm, &keyMap);
	}
	//std::cerr << "    Done\n";
}

template <typename K>
void faster::workerFddGroup<K>::preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	fastCommBuffer &buffer = comm->getResultBuffer();
	size_t durationP;
	size_t rSizeP;
	size_t headerSize;

	buffer.reset();
	buffer << id;

	// Reserve space for the time duration
	durationP = buffer.size();
	buffer.advance(sizeof(size_t));

	rSizeP = buffer.size();
	buffer.advance(sizeof(size_t));

	headerSize = buffer.size();

	auto start = system_clock::now();
	if (op & (OP_GENERICMAP | OP_GENERICREDUCE | OP_GENERICUPDATE)){
		apply(func, op, dest, buffer);

		//std::cerr << "         RSIZE:" << size_t(dest->getSize());
		buffer << size_t(dest->getSize());
	}else{
		if (op == OP_CoGroup){
			cogroup(comm);
		}
	}
	auto end = system_clock::now();

	auto duration = duration_cast<milliseconds>(end - start);
	//std::cerr << "      ET:" << duration.count();

	buffer.writePos(duration.count(), durationP);
	buffer.writePos(buffer.size() - headerSize, rSizeP);

	comm->sendTaskResult();
	//std::cerr << "      DONE\n";
}


