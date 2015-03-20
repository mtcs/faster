#include <chrono>
#include <omp.h>
#include <algorithm>
#include <chrono>

#include "workerFddGroup.h"
#include "workerFdd.h"
#include "fastComm.h"
#include "indexedFddStorageExtern.cpp"
#include "misc.h"

	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

template <typename K>
faster::workerFddGroup<K>::workerFddGroup(unsigned long int id, fddType keyT, std::vector< workerFddBase* > & members): workerFddBase(id, Null){
	keyType = keyT;
	this->members = members;
}


template <typename K>
std::vector< std::vector<void*>> faster::workerFddGroup<K>::findKeyInterval(size_t i){
	//auto start = system_clock::now();
	faster::workerFddBase * wfdd = members[i];
	char * data = (char*) wfdd->getData();
	K * keys = (K *) wfdd->getKeys();
	size_t fddSize = wfdd->getSize();
	int baseSize = wfdd->baseSize();

	std::unordered_map<K, int> keyCount(wfdd->getSize()*1.2);
	for ( size_t i = 0; i < fddSize; ++i){
		keyCount[keys[i]] ++;
	}
	
	//auto t0 =  duration_cast<milliseconds>(system_clock::now() - start).count();
	//auto start2 = system_clock::now();

	std::unordered_map<K, std::vector<void*>> keyLocations(keyCount.size()*1.2);
	for ( auto it = keyCount.cbegin(); it != keyCount.end(); ++it ){
		keyLocations[it->first].reserve(it->second);
	}
	keyCount.clear();

	//auto t1 =  duration_cast<milliseconds>(system_clock::now() - start2).count();
	//start2 = system_clock::now();

	for ( size_t i = 0; i < fddSize; ++i){
		K & key = keys[i];
		void * d = &data[baseSize * i];
		auto locs = keyLocations.find(key);
		if (locs != keyLocations.end()){
			locs->second.insert(locs->second.end(), d);
		}
	}

	//auto t3 =  duration_cast<milliseconds>(system_clock::now() - start2).count();
	//start2 = system_clock::now();

	std::vector<std::vector<void*>> keyLocationsV(uKeys->size());

	//std::cerr << "FKI[" ;
	// Move Data locations to the result vector
	for ( size_t i = 0; i < uKeys->size() ; ++i){
		auto l = keyLocations.find((*uKeys)[i]);

		if ( l == keyLocations.end() ){
			keyLocationsV[i] = {};
		}else{
			//std::cerr << (*uKeys)[i] << ":" << l->second.size() << "  "; 
			//keyLocationsV[i] = std::move(l->second);
			keyLocationsV[i] = l->second;
		}
	}
	//std::cerr << "]FKI\n" ;
	//std::cerr << "T1:" << t1 << " TOT:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
	//std::cerr << "FindKeyInterval T0:" << t0 << " t1:" << t1 << " t2:" << t2 << " t3:" << t3 << " " <<  duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	return keyLocationsV;
}

template <typename K>
std::vector< std::vector<void*> > * faster::workerFddGroup<K>::getMemberKeyLocations(size_t i){
	std::vector< std::vector<void*> > * location;

	location = members[i]->getKeyLocations();

	if ( location->size() == 0 ){
		(*location) = findKeyInterval(i);
		//auto loc = findKeyInterval(i);
		//location->resize(loc.size());
		
		//for ( size_t j = 0; j < loc.size(); j++ ){
			//(*location)[j] = std::move(loc[j]);
			//loc[j].clear();
		//}
		//loc.clear();
	}

	return location;
}

template <typename K>
void faster::workerFddGroup<K>::updateByKey(void * func){

	//std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::vector< std::vector<void*> > * keyLocations[3];

	for ( size_t i = 0; i < members.size(); ++i){
		keyLocations[i] = getMemberKeyLocations(i);
	}

	if ( members.size() < 3 ){  
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			((updateByKeyG2FunctionP<K>) func) 
				((*uKeys)[i], 
				 (*keyLocations[0])[i],
				 (*keyLocations[1])[i]);
		}
	}else{
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			((updateByKeyG3FunctionP<K>) func) 
				((*uKeys)[i], 
				 (*keyLocations[0])[i], 
				 (*keyLocations[1])[i], 
				 (*keyLocations[2])[i] );
		}
	}
}

template <typename K>
void faster::workerFddGroup<K>::bulkUpdate(void * func){
	if ( members.size() < 3 ){  
		((bulkUpdateG2FunctionP<K>) func) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize()
			);
	}else{
		((bulkUpdateG3FunctionP<K>) func) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize(),
			 (K*) members[2]->getKeys(), members[2]->getData(), members[2]->getSize()
			);
	}
}


template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::mapByKey(workerFddBase * dest, void * func){
	//std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::vector< std::vector<void*> > * keyLocations[3];
	
	//std::cerr << "START " << id << " " << s << "  ";

	for ( size_t i = 0; i < members.size(); ++i){
		keyLocations[i] = getMemberKeyLocations(i);
	}
	dest->setSize(uKeys->size());
	U * od = (U*) dest->getData();

	if ( members.size() < 3 ){  
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			od[i] = ((mapByKeyG2FunctionP<K,U>) func)
				( (*uKeys)[i], 
				  (*keyLocations[0])[i],
				  (*keyLocations[1])[i]);

		}
	}else{
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			od[i] = ((mapByKeyG3FunctionP<K,U>) func)
				( (*uKeys)[i], 
				  (*keyLocations[0])[i], 
				  (*keyLocations[1])[i], 
				  (*keyLocations[2])[i] );

		}
	}
	//std::cerr << "END ";
}		

template <typename K>
//template <typename L, typename U, typename T0, typename T1, typename T2>
template <typename L, typename U>
void faster::workerFddGroup<K>::mapByKeyI(workerFddBase * dest, void * func){
	//std::cerr << "    \033[0;33mMap By Key\033[0m\n";
	//std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::vector< std::vector<void*> > * keyLocations[3];


	//std::cerr << "       FindKeyInterval ";
	for ( size_t i = 0; i < members.size(); ++i){
		keyLocations[i] = getMemberKeyLocations(i);
	}
	//std::cerr << "\n";
	//std::cerr << "       SetSize: " << uKeys->size() << "\n";

	dest->setSize(uKeys->size());
	L * ok = (L*) dest->getKeys();
	U * od = (U*) dest->getData();


	//std::cerr << "       Run\n";
	if ( members.size() < 3 ){  
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			std::pair<L,U> r = ( (ImapByKeyG2FunctionP<K,L,U>) func) 
				( (*uKeys)[i], 
				  (*keyLocations[0])[i], 
				  (*keyLocations[1])[i] );
			ok[i] = r.first;
			od[i] = r.second;

		}
	}else{
		#pragma omp parallel for 
		for ( size_t i = 0; i < uKeys->size(); ++i){
			std::pair<L,U> r = ( (ImapByKeyG3FunctionP<K,L,U>) func )
				( (*uKeys)[i], 
				(*keyLocations[0])[i], 
				(*keyLocations[1])[i], 
				(*keyLocations[2])[i] );
			ok[i] = r.first;
			od[i] = r.second;

		}
	}

	//std::cerr << "    DONE\n";
	//std::cerr << "END ";
}		

template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::flatMapByKey(workerFddBase * dest, void * func){
	//std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::vector< std::vector<void*> > * keyLocations[3];
	std::deque<U> resultList;

	for ( size_t i = 0; i < members.size(); ++i){
		keyLocations[i] = getMemberKeyLocations(i);
	}

	if ( members.size() < 3 ){  
		#pragma omp parallel 
		{
			std::deque<U> pResultList;

			#pragma omp for 
			for ( size_t i = 0; i < uKeys->size(); ++i){
				std::deque<U> r = ((flatMapByKeyG2FunctionP<K,U>) func)
					( (*uKeys)[i], 
					(*keyLocations[0])[i], 
					(*keyLocations[1])[i] );


				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());
			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}
	}else{
		#pragma omp parallel 
		{
			std::deque<U> pResultList;

			#pragma omp for 
			for ( size_t i = 0; i < uKeys->size(); ++i){
				std::deque<U> r = ((flatMapByKeyG3FunctionP<K,U>) func)
					( (*uKeys)[i], 
					(*keyLocations[0])[i], 
					(*keyLocations[1])[i], 
					(*keyLocations[2])[i] );


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
void faster::workerFddGroup<K>::flatMapByKeyI(workerFddBase * dest, void * func){
	//std::unordered_map<K, std::pair<void*, size_t>> keyLocations[3];
	std::vector< std::vector<void*> > * keyLocations[3];
	std::deque<std::pair<L,U>> resultList;

	//std::cerr << "START " << id << " S:" << uKeys->size() << " \n";


	//std::cerr << "KL ";
	for ( size_t i = 0; i < members.size(); ++i){
		keyLocations[i] = getMemberKeyLocations(i);
	}

	if ( members.size() <3 ){  
		#pragma omp parallel 
		{
			std::deque<std::pair<L,U>> pResultList;

			#pragma omp  for 
			for ( size_t i = 0; i < uKeys->size(); ++i){
				std::deque<std::pair<L,U>> r = ( (IflatMapByKeyG2FunctionP<K,L,U>) func) 
					( (*uKeys)[i], 
					(*keyLocations[0])[i], 
					(*keyLocations[1])[i] );


				if (r.size() > 0)
					pResultList.insert(pResultList.end(), r.begin(), r.end());
			}

			#pragma omp critical
			resultList.insert(resultList.end(), pResultList.begin(), pResultList.end());
		}
	}else{
		#pragma omp parallel 
		{
			std::deque<std::pair<L,U>> pResultList;

			#pragma omp  for 
			for ( size_t i = 0; i < uKeys->size(); ++i){
				std::deque<std::pair<L,U>> r = ( (IflatMapByKeyG3FunctionP<K,L,U>) func )
					( (*uKeys)[i], 
					(*keyLocations[0])[i], 
					(*keyLocations[1])[i], 
					(*keyLocations[2])[i] );


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
template <typename U>
void faster::workerFddGroup<K>::bulkFlatMap(workerFddBase * dest, void * func){
	std::deque<U> resultList;
	
	if ( members.size() < 3 ){  
		resultList = ( (bulkFlatMapG2FunctionP<K,U>) func ) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize()
			);
	}else{
		resultList = ( (bulkFlatMapG3FunctionP<K,U>) func ) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize(),
			 (K*) members[2]->getKeys(), members[2]->getData(), members[2]->getSize()
			);
	}

	dest->insertl(&resultList);
}

template <typename K>
template <typename L, typename U>
void faster::workerFddGroup<K>::bulkFlatMapI(workerFddBase * dest, void * func){
	std::deque<std::pair<L,U>> resultList;
	
	if ( members.size() < 3 ){  
		resultList = ( (IbulkFlatMapG2FunctionP<K,L,U>) func ) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize()
			);
	}else{
		resultList = ( (IbulkFlatMapG3FunctionP<K,L,U>) func ) 
			(
			 (K*) members[0]->getKeys(), members[0]->getData(), members[0]->getSize(),
			 (K*) members[1]->getKeys(), members[1]->getData(), members[1]->getSize(),
			 (K*) members[2]->getKeys(), members[2]->getData(), members[2]->getSize()
			);
	}

	dest->insertl(&resultList);
	//std::cerr << "      NewSize = " <<  dest->getSize() << "\n";
}

template <typename K>
//template <typename U, typename T0, typename T1, typename T2>
template <typename U>
void faster::workerFddGroup<K>::_apply(void * func, fddOpType op, workerFddBase * dest){ 
	switch (op){
		case OP_MapByKey:
			//std::cerr << "        MapByKey \n";
			mapByKey<U>(dest, func);
			break;
		case OP_FlatMapByKey:
			//std::cerr << "        FlatMapByKey \n";
			flatMapByKey<U>(dest, func);
			break;
		case OP_BulkFlatMap:
			//std::cerr << "        BulkFlatMap \n";
			bulkFlatMap<U>(dest, func);
			break;
	}
}

template <typename K>
//template <typename L, typename U, typename T0, typename T1, typename T2>
template <typename L, typename U>
void faster::workerFddGroup<K>::_applyI(void * func, fddOpType op, workerFddBase * dest){ 
	switch (op){
		case OP_MapByKey:
			//std::cerr << "        MapByKeyI \n";
			mapByKeyI<L,U>(dest, func);
			break;
		case OP_FlatMapByKey:
			//std::cerr << "        FlatMapByKeyI \n";
			flatMapByKeyI<L,U>(dest, func);
			break;
		case OP_BulkFlatMap:
			//std::cerr << "        BulkFlatMapI \n";
			bulkFlatMapI<L,U>(dest, func);
			break;
	}
}

template <typename K>
//template <typename T0, typename T1, typename T2>
void faster::workerFddGroup<K>::_applyReduce(void * func UNUSED, fddOpType op UNUSED, fastCommBuffer & buffer){
	void * r = NULL;
	size_t rSize = sizeof(r);

	switch (op){
		case OP_UpdateByKey:
			//std::cerr << "        Update \n";
			updateByKey(func);
			break;
		case OP_BulkUpdate:
			//std::cerr << "        BulkUpdate \n";
			bulkUpdate(func);
			break;
		/*case OP_Reduce:
			r = reduce( ( reduceGFunctionP<T> ) func);
			//std::cerr << "Reduce ";
			break;
		case OP_BulkReduce:
			r = bulkReduce( ( bulkReduceGFunctionP<T> ) func);
			//std::cerr << "BulkReduce ";
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
		case String:    _applyI<L, std::string> (func, op, dest); break;
		case CharV:     _applyI<L, std::vector<char>> 	(func, op, dest); break;
		case IntV:      _applyI<L, std::vector<int>> 	(func, op, dest); break;
		case LongIntV:  _applyI<L, std::vector<long int>>(func, op, dest); break;
		case FloatV:    _applyI<L, std::vector<float>> 	(func, op, dest); break;
		case DoubleV:   _applyI<L, std::vector<double>> (func, op, dest); break;
	}
}


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
	//std::cerr << "        Cogroup";
	//auto start = system_clock::now();
	fastCommBuffer &buffer = comm->getResultBuffer();

	//std::cerr << "      RecvKeyMap\n";
	//comm->recvCogroupData(tid, keyMap, regroup);
	//std::cerr << " RcvUK:" << duration_cast<milliseconds>(system_clock::now() - start).count() << " ";
	//start = system_clock::now();

	//std::cerr << "        My Keys: ";
	this->uKeys = *(std::shared_ptr<std::vector<K>>*) members[0]->getUKeys();
	this->keyMap = *(std::shared_ptr<std::unordered_map<K, int>>*) members[0]->getKeyMap();
	//for( auto it = keyMap.begin(); it != keyMap.end(); it++ ){
	//	if ( it->second == comm->getProcId() ){
	//		//std::cerr << "\033[0;31m";
	//		uKeys->insert(uKeys->end(), it->first);
	//	}
	//	//std::cerr  << it->first << "\033[0m ";
	//}
	//std::cerr << " SaveUK:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
	//start = system_clock::now();
	//std::cerr << "\n";

	//uKeys->shrink_to_fit();

	//std::sort(uKeys->begin(), uKeys->end());

	//std::cerr << "        Exchange Data By Key: ";
	for ( size_t i = 1; i < members.size(); ++i){
		auto memberUKeys = *(std::shared_ptr<std::vector<K>>*) members[i]->getUKeys();
		if ( memberUKeys != uKeys ){
			//std::cerr << i << " ";
			members[i]->setUKeys( & this->uKeys );
			members[i]->setKeyMap( & this->keyMap );

			members[i]->exchangeDataByKey(comm);
			//std::cerr << " REGROUP " << i << "\n";
			//std::cerr << "Tx:" << duration_cast<milliseconds>(system_clock::now() - start).count() << " ";
			//start = system_clock::now();
		}
		buffer << members[i]->getSize();
	}
	//std::cerr << "\n";
	//std::cerr << " EDBK:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
}

template <typename K>
void faster::workerFddGroup<K>::preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	fastCommBuffer &buffer = comm->getResultBuffer();
	size_t durationP;
	size_t rSizeP;
	size_t rStatP;
	size_t headerSize;
	size_t ret = 0;
	procstat s;

	buffer.reset();
	buffer << id;

	// Reserve space for the time duration
	durationP = buffer.size();
	buffer.advance(sizeof(size_t));

	rStatP = buffer.size();
	buffer.advance(s);

	rSizeP = buffer.size();
	buffer.advance(sizeof(size_t));

	headerSize = buffer.size();

	auto start = system_clock::now();
	if (op & (OP_GENERICMAP | OP_GENERICREDUCE | OP_GENERICUPDATE)){
		apply(func, op, dest, buffer);

		//std::cerr << "         RSIZE:" << size_t(dest->getSize());
		if (dest) buffer << size_t(dest->getSize());
	}else{
		if (op == OP_CoGroup){
			cogroup(comm);
		}
		buffer << ret;
	}
	auto end = system_clock::now();
	auto duration = duration_cast<milliseconds>(end - start);
	//std::cerr << "      ET:" << duration.count();

	buffer.writePos(size_t(duration.count()), durationP);
	buffer.writePos(getProcStat(), rStatP);
	buffer.writePos(size_t(buffer.size() - headerSize), rSizeP);

	comm->sendTaskResult();
}


