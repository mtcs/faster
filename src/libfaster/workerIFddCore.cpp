#include "workerIFdd.h"
#include "fastComm.h"
#include "indexedFddStorageExtern.cpp"

using namespace faster;

template <typename K, typename T>
faster::workerIFddCore<K,T>::workerIFddCore(unsigned int ident, fddType t) : workerFddBase(ident, t){
	localData = new indexedFddStorage<K,T>();
} 


template <typename K, typename T>
faster::workerIFddCore<K,T>::workerIFddCore(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
	localData = new indexedFddStorage<K,T>(size);
}


template <typename K, typename T>
faster::workerIFddCore<K,T>::~workerIFddCore(){
	delete localData;
	delete resultBuffer;
}

template <typename K, typename T>
fddType faster::workerIFddCore<K,T>::getType() { 
	return type; 
}

template <typename K, typename T>
fddType faster::workerIFddCore<K,T>::getKeyType() { 
	return keyType; 
}


template <typename K, typename T>
T & faster::workerIFddCore<K,T>::operator[](size_t address){ 
	return this->localData->getData()[address]; 
}

template <typename K, typename T>
void * faster::workerIFddCore<K,T>::getData(){ 
	return this->localData->getData(); 
}

template <typename K, typename T>
void * faster::workerIFddCore<K,T>::getKeys(){ 
	return this->localData->getKeys(); 
}

template <typename K, typename T>
size_t faster::workerIFddCore<K,T>::getSize(){ 
	return this->localData->getSize(); 
}

template <typename K, typename T>
size_t faster::workerIFddCore<K,T>::itemSize(){ 
	return sizeof(T); 
}

template <typename K, typename T>
size_t faster::workerIFddCore<K,T>::baseSize(){ 
	return sizeof(T); 
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::deleteItem(void * item) { 
	delete (T*) item; 
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::shrink(){ 
	localData->shrink(); 
}


template <typename K, typename T>
K * faster::workerIFddCore<K,T>::distributeOwnership(fastComm * comm, K * uKeys, size_t cSize){
	size_t keyPerProc = cSize/(comm->getNumProcs() - 1);
	int procId = comm->getProcId();
	K * recvOwnership = new K[comm->getNumProcs()];

	// Send Ownership start sugestion to others
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i != procId){
			comm->sendKeyOwnershipSugest(i, uKeys[i * keyPerProc]);
		}
	}

	// Recv Key onwership sugestions
	comm->recvKeyOwnershipSugest(recvOwnership);
	
	// Find out what will be my ownership
	K myOwnership = recvOwnership[procId * keyPerProc];
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i != procId){
			// If is the first node get the minimum
			if (myOwnership < recvOwnership[i])
				myOwnership = recvOwnership[i];
		}
	}
	
	// Send my ownership to others
	comm->sendMyKeyOwnership(myOwnership);
	
	// Receive all ownerships
	comm->recvAllKeyOwnership(recvOwnership);
	recvOwnership[procId - 1] = myOwnership;

	return recvOwnership;
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::sendPartKeyCount(fastComm *comm){
	K * keys = localData->getKeys();
	size_t size = localData->getSize();
	K * uKeys;
	K * recvOwnership;

	// This needs to be ordered to send in order
	std::map<K, size_t> count;
	// Count keys
	for (int i = 0; i < size; ++i){
		if (count.find(keys[i]) == count.end()) 
			count[keys[i]] = 1;
		else 
			count[keys[i]]++;
	}
	
	// Convert map to array
	uKeys = new K[count.size()];
	int i = 0;
	for ( auto it = count.begin(); it != count.end(); it++){
		uKeys[i] = it->first;
	}

	recvOwnership = distributeOwnership(comm, uKeys, count.size());


	// Send KeyCount to key owner
	i = 2;
	size_t numKeys = 0;
	comm->buffer[i].reset();
	comm->buffer[i] << numKeys;
	for ( auto it = count.begin(); it != count.end(); it++){
		// If key passed the current node ownership, send message and advance 
		if ((i < comm->getNumProcs()) && (it->first > recvOwnership[i])){
			comm->sendMyKeyCount(i, numKeys);
			comm->buffer[i].reset();
			i++;
			numKeys	= 0;;
			comm->buffer[i] << numKeys;
		}
		numKeys++;
		comm->buffer[i] << it->first << it->second;
	}
	comm->sendMyKeyCount(i, numKeys);
}

template <typename K, typename T>
CountKeyMapT<K> faster::workerIFddCore<K,T>::recvPartKeyMaxCount(fastComm *comm, PPCountKeyMapT<K> & keyPPMaxCount){
	// Recv KeyCounts
	int src;
	
	CountKeyMapT<K> keyCount;
	for ( int i = 0; i < (comm->getNumProcs() - 2); ++i){
		// Recv a process count for the key that I own
		std::list<std::pair<K,size_t>> countList = comm->recvMyKeyCount<K>(src);

		// Verify if it is the process with most keys
		for ( auto it = countList.begin(); it != countList.end(); it++){
			K key = it->first;
			size_t recvCount = it->second;
			auto item = keyCount.find(it->first);
			
			if (item != keyCount.end()){
				size_t maxCount = keyPPMaxCount[key].first;
				if (recvCount > maxCount){
					keyPPMaxCount[key].second.clear();
					keyPPMaxCount[key].second.push_back(src);
				}else{
					if (recvCount == maxCount){
						keyPPMaxCount[key].second.push_back(src);
					}
				}
				keyCount[key] += recvCount;
			}else{
				keyCount[key] = recvCount;
				keyPPMaxCount[key].first = recvCount;
				keyPPMaxCount[key].second.push_back(src);
			}
		}
	}
	return keyCount;
}

template <typename K, typename T>
CountKeyMapT<K> faster::workerIFddCore<K,T>::recvPartKeyCount(fastComm *comm){
	// Recv KeyCounts
	int src;
	CountKeyMapT<K> keyCount;
	for ( int i = 0; i < (comm->getNumProcs() - 2); ++i){
		// Recv a process count for the key that I own
		std::list<std::pair<K,size_t>> countList = comm->recvMyKeyCount<K>(src);

		// Verify if it is the process with most keys
		for ( auto it = countList.begin(); it != countList.end(); it++){
			K key = it->first;
			size_t recvCount = it->second;
			auto item = keyCount.find(it->first);
			
			if (item != keyCount.end()){
				keyCount[key] += recvCount;
			}else{
				keyCount[key] = recvCount;
			}
		}
	}
	return keyCount;
}

template <typename K, typename T>
CountKeyMapT<K> faster::workerIFddCore<K,T>::distributedMaxKeyCount(fastComm *comm, PPCountKeyMapT<K> & keyPPMaxCount){

	sendPartKeyCount(comm);

	CountKeyMapT<K> keyCount = recvPartKeyMaxCount(comm, keyPPMaxCount);

	return keyCount;
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::countByKey(fastComm *comm){

	sendPartKeyCount(comm);

	CountKeyMapT<K> keyCount = recvPartKeyCount(comm);

	comm->sendCountByKey(keyCount);
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::groupByKey(fastComm *comm){
	PPCountKeyMapT<K> keyMaxCount;

	// Get Unique Keys Counts
	CountKeyMapT<K>  count = distributedMaxKeyCount(comm, keyMaxCount);
	
	// Decide who will get my keys
	//size_t itemsPerProc = localData->getSize() / ( comm->getNumProcs() - 1 );
	//std::unordered_map<K, bool> assigned;

	for(auto it = count.begin(); it != count.end(); it++){
		K key = it->first;
		int bestCandidate UNUSED = keyMaxCount[key].second.front();
	}
	
	// Send Items + keys
	// Recv Items + keys

	groupedByKey = true;
}



template class faster::workerIFddCore<char, char>;
template class faster::workerIFddCore<char, int>;
template class faster::workerIFddCore<char, long int>;
template class faster::workerIFddCore<char, float>;
template class faster::workerIFddCore<char, double>;
template class faster::workerIFddCore<char, char*>;
template class faster::workerIFddCore<char, int*>;
template class faster::workerIFddCore<char, long int*>;
template class faster::workerIFddCore<char, float*>;
template class faster::workerIFddCore<char, double*>;
template class faster::workerIFddCore<char, std::string>;
template class faster::workerIFddCore<char, std::vector<char>>;
template class faster::workerIFddCore<char, std::vector<int>>;
template class faster::workerIFddCore<char, std::vector<long int>>;
template class faster::workerIFddCore<char, std::vector<float>>;
template class faster::workerIFddCore<char, std::vector<double>>;

template class faster::workerIFddCore<int, char>;
template class faster::workerIFddCore<int, int>;
template class faster::workerIFddCore<int, long int>;
template class faster::workerIFddCore<int, float>;
template class faster::workerIFddCore<int, double>;
template class faster::workerIFddCore<int, char*>;
template class faster::workerIFddCore<int, int*>;
template class faster::workerIFddCore<int, long int*>;
template class faster::workerIFddCore<int, float*>;
template class faster::workerIFddCore<int, double*>;
template class faster::workerIFddCore<int, std::string>;
template class faster::workerIFddCore<int, std::vector<char>>;
template class faster::workerIFddCore<int, std::vector<int>>;
template class faster::workerIFddCore<int, std::vector<long int>>;
template class faster::workerIFddCore<int, std::vector<float>>;
template class faster::workerIFddCore<int, std::vector<double>>;

template class faster::workerIFddCore<long int, char>;
template class faster::workerIFddCore<long int, int>;
template class faster::workerIFddCore<long int, long int>;
template class faster::workerIFddCore<long int, float>;
template class faster::workerIFddCore<long int, double>;
template class faster::workerIFddCore<long int, char*>;
template class faster::workerIFddCore<long int, int*>;
template class faster::workerIFddCore<long int, long int*>;
template class faster::workerIFddCore<long int, float*>;
template class faster::workerIFddCore<long int, double*>;
template class faster::workerIFddCore<long int, std::string>;
template class faster::workerIFddCore<long, std::vector<char>>;
template class faster::workerIFddCore<long, std::vector<int>>;
template class faster::workerIFddCore<long, std::vector<long int>>;
template class faster::workerIFddCore<long, std::vector<float>>;
template class faster::workerIFddCore<long, std::vector<double>>;

template class faster::workerIFddCore<float, char>;
template class faster::workerIFddCore<float, int>;
template class faster::workerIFddCore<float, long int>;
template class faster::workerIFddCore<float, float>;
template class faster::workerIFddCore<float, double>;
template class faster::workerIFddCore<float, char*>;
template class faster::workerIFddCore<float, int*>;
template class faster::workerIFddCore<float, long int*>;
template class faster::workerIFddCore<float, float*>;
template class faster::workerIFddCore<float, double*>;
template class faster::workerIFddCore<float, std::string>;
template class faster::workerIFddCore<float, std::vector<char>>;
template class faster::workerIFddCore<float, std::vector<int>>;
template class faster::workerIFddCore<float, std::vector<long int>>;
template class faster::workerIFddCore<float, std::vector<float>>;
template class faster::workerIFddCore<float, std::vector<double>>;

template class faster::workerIFddCore<double, char>;
template class faster::workerIFddCore<double, int>;
template class faster::workerIFddCore<double, long int>;
template class faster::workerIFddCore<double, float>;
template class faster::workerIFddCore<double, double>;
template class faster::workerIFddCore<double, char*>;
template class faster::workerIFddCore<double, int*>;
template class faster::workerIFddCore<double, long int*>;
template class faster::workerIFddCore<double, float*>;
template class faster::workerIFddCore<double, double*>;
template class faster::workerIFddCore<double, std::string>;
template class faster::workerIFddCore<double, std::vector<char>>;
template class faster::workerIFddCore<double, std::vector<int>>;
template class faster::workerIFddCore<double, std::vector<long int>>;
template class faster::workerIFddCore<double, std::vector<float>>;
template class faster::workerIFddCore<double, std::vector<double>>;

template class faster::workerIFddCore<std::string, char>;
template class faster::workerIFddCore<std::string, int>;
template class faster::workerIFddCore<std::string, long int>;
template class faster::workerIFddCore<std::string, float>;
template class faster::workerIFddCore<std::string, double>;
template class faster::workerIFddCore<std::string, char*>;
template class faster::workerIFddCore<std::string, int*>;
template class faster::workerIFddCore<std::string, long int*>;
template class faster::workerIFddCore<std::string, float*>;
template class faster::workerIFddCore<std::string, double*>;
template class faster::workerIFddCore<std::string, std::string>;
template class faster::workerIFddCore<std::string, std::vector<char>>;
template class faster::workerIFddCore<std::string, std::vector<int>>;
template class faster::workerIFddCore<std::string, std::vector<long int>>;
template class faster::workerIFddCore<std::string, std::vector<float>>;
template class faster::workerIFddCore<std::string, std::vector<double>>;

