#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <string>
#include <iomanip>
#include <unistd.h>

#include "_workerIFdd.h"
#include "fastComm.h"
#include "misc.h"
#include "hasher.h"
#include "indexedFddStorageExtern.cpp"

using namespace faster;

template <typename K, typename T>
faster::workerIFddCore<K,T>::workerIFddCore(unsigned int ident, fddType kt, fddType t) : workerFddBase(ident, t){
	keyType = kt;
	localData = new indexedFddStorage<K,T>();
} 


template <typename K, typename T>
faster::workerIFddCore<K,T>::workerIFddCore(unsigned int ident, fddType kt, fddType t, size_t size) : workerFddBase(ident, t){ 
	keyType = kt;
	localData = new indexedFddStorage<K,T>(size);
}


template <typename K, typename T>
faster::workerIFddCore<K,T>::~workerIFddCore(){
	keyLocations.clear();
	delete localData;
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
void faster::workerIFddCore<K,T>::setSize(size_t s) { 
	localData->setSize(s); 
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
void faster::workerIFddCore<K,T>::countByKey(fastComm *comm){
	//std::cerr << "        CountByKey\n";
	K * keys = localData->getKeys();
	size_t size = localData->getSize();
	fastCommBuffer &buffer = comm->getResultBuffer();

	// TODO PARALELIZE!
	// This needs to be ordered to send in order
	std::unordered_map<K, size_t> count;
	// Count keys
	for ( size_t i = 0; i < size; ++i){
		//typename std::unordered_map<K, size_t>::iterator it = count.find(keys[i]);
		//if (count.find(keys[i]) == count.end()) 
			//count[keys[i]] = 1;
		//else 
			count[keys[i]] += 1;
	}
	buffer << size_t(count.size());
	int sum = 0;
	for ( auto it = count.begin(); it != count.end(); it++){
		sum += it->second;
		buffer << it->first << it->second;
	}

}

//void p(double & v){
	//fprintf(stderr, "%.2lf ", v); 
//}

//template <typename T>
//void p(T & v UNUSED){
//}
template <typename K, typename T>
bool faster::workerIFddCore<K,T>::EDBKSendData(fastComm *comm, std::vector<size_t> & dataSize){
	bool tryShrink = false;
	fastCommBuffer * buffer = comm->getSendBuffers();

	// Include the data size in the message header
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i == comm->getProcId())
			continue;
		if (dataSize[i] > 0)
			tryShrink = true;

		buffer[i].writePos(dataSize[i], 0);
		//buffer[i].writePos(0, 1, sizeof(size_t));
		comm->sendGroupByKeyData(i);
		//std::cerr << "\033[0;31m"<< dataSize[i] << " \033[0m> " << i << "  ";
	}
	//std::cerr << "]\n";

	//Ti[1] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();
	
	return tryShrink;

}
template <typename K, typename T>
bool faster::workerIFddCore<K,T>::EDBKRecvData(fastComm *comm, std::vector<bool> & deleted, size_t & pos, bool tryShrink){
	K * keys = localData->getKeys();
	T * data = localData->getData();
	bool dirty = false;
	
	// Find the first emty space
	pos = 0;
	if (tryShrink){
		while ( (pos < deleted.size() ) && ( ! deleted[pos] ) )
			pos++;
	}else{
		pos = deleted.size();
	}
	

	// Recv all keys I own in-place
	//std::cerr << comm->getProcId() << "        Recv data In-place\n";
	//std::cerr << comm->getProcId() << "          Insert: [";
	for ( int i = 1; i < (comm->getNumProcs() - 1); ++i){
		int rSize;
		size_t numItems;
		fastCommBuffer rb(0);
		//char cont = 1;

		//while (!cont){	

		void * rData = comm->recvGroupByKeyData(rSize);
		rb.setBuffer(rData, rSize);

		rb >> numItems ; // >> cont;
		if (numItems > 0)
			dirty = true;

		//std::cerr << "| \033[0;32m" << numItems << "\033[0m - ";

		for (size_t i = 0; i < numItems; ++i){
			// Find a empty space in the local data
			while ( (pos < deleted.size() ) && ( ! deleted[pos] ) )
				pos++;
			// Make sure it is not out of bounds, if it is, grow local data to fit new data
			if( pos >= localData->getSize() ){
				localData->setSize(localData->getSize() + numItems - i);
				data = localData->getData();
				keys = localData->getKeys();
				//std::cerr << "( GROW: "<< localData->getSize() << " ) ";
			}
			//p(data[pos]);
			rb >> keys[pos] >> data[pos];
			//std::cerr  << keys[pos] << ">" << pos << " ";
			//std::cerr  << keys[pos] << " ";
			pos++;
		}
		//}
	}
	//std::cerr << "]\n";
	comm->waitForReq(comm->getNumProcs() - 2);

	return dirty;
}
template <typename K, typename T>
void faster::workerIFddCore<K,T>::EDBKShrinkData(std::vector<bool> & deleted, size_t pos, bool tryShrink ){
	K * keys = localData->getKeys();
	T * data = localData->getData();

	//Ti[2] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	// If there are elements that are still sparse in the memory
	if ( tryShrink && ( pos < deleted.size() ) ){
		//std::cerr << "        [ Shrink: (" << pos << "," << deleted.size() << " ";
		// Bring them forward and correct size
		for (size_t i = (pos); i < localData->getSize(); ++i){
			// Found sparse a item
			//std::cerr << " " << i;
			if ( ! deleted[i] ) {
				if (i > pos) {
					keys[pos] = keys[i];
					data[pos] = data[i];
					//std::cerr << ">" << pos;
				}
				pos++;
			}
		}
		localData->setSize(pos);
		//std::cerr << " ]\n" ;
	}

}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::exchangeDataByKeyMapped(fastComm *comm){
	//using std::chrono::system_clock;
	//using std::chrono::duration_cast;
	//using std::chrono::milliseconds;

	//int Ti[5];
	//auto start = system_clock::now();
	//std::cerr << "    \033[0;33mExchange Data By Key\033[0m\n";
	K * keys = localData->getKeys();
	T * data = localData->getData();
	size_t size = localData->getSize();
	fastCommBuffer * buffer = comm->getSendBuffers();
	std::vector<size_t> dataSize(comm->getNumProcs(), 0);
	std::vector<bool> deleted(size, false);
	size_t pos;
	bool dirty = false;
	bool tryShrink = false;


	
	//Ti[0] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	//std::cerr << comm->getProcId() << "        Write Buffers";
	// Reserve space in the message for the header
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i == comm->getProcId())
			continue;
		buffer[i].reset();
		buffer[i].advance(sizeof(size_t));
		//buffer[i].advance(1);
	}
	//std::cerr << "\n";

	//std::cerr << comm->getProcId() << "        [ ";
	// Insert Data that dont belong to me in the message
	for ( size_t i = 0; i < size; ++i){
		K & key = keys[i];
		int owner = (*keyMap)[key];
		
		//if (owner != comm->getProcId())
			//std::cerr << "\033[0;31m";
		//std::cerr << " " << key << "\033[0m";

		if (owner == comm->getProcId())
			continue;
		buffer[owner] << key <<  data[i];
		dataSize[owner]++;
		deleted[i] = true;
		//p(data[i]);
		//std::cerr << ":" << i;
		//std::cerr << i << ":" << key << ">" << owner << "  ";
		//std::cerr << i << ":" << (size_t) data[i] << " ";
	}

	//std::cerr << " - ";


	comm->joinSlaves();

	// Send Data
	tryShrink = EDBKSendData(comm, dataSize);

	// Recv Data
	dirty = EDBKRecvData(comm, deleted, pos, tryShrink);

	// Fit data do memmory
	EDBKShrinkData(deleted, pos, tryShrink);
	//std::cerr << "        (new size: " << localData->getSize() << ")\n";
	
	// Clear Key location saved by last ByKey function
	if ( dirty | tryShrink ){
		if (keyLocations.size() > 0){
			keyLocations.clear();
			//std::cerr << " CLEAR KeyLocations\n" ;
		}
	}
	//Ti[3] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//std::cerr << " TIn:" << Ti[0] << " TSn:" << Ti[1] << " TRv:" <<  Ti[2] << " TSh:" << Ti[3] << "\n";

}
template <typename K, typename T>
bool faster::workerIFddCore<K,T>::exchangeDataByKeyHashed(fastComm *comm){
	//using std::chrono::system_clock;
	//using std::chrono::duration_cast;
	//using std::chrono::milliseconds;

	//int Ti[5];
	//auto start = system_clock::now();
	//std::cerr << "    \033[0;33mExchange Data By Key\033[0m\n";
	K * keys = localData->getKeys();
	T * data = localData->getData();
	size_t size = localData->getSize();
	fastCommBuffer * buffer = comm->getSendBuffers();
	std::vector<size_t> dataSize(comm->getNumProcs(), 0);
	std::vector<bool> deleted(size, false);
	size_t pos;
	bool dirty = false;
	bool tryShrink = false;

	hasher<K> hash(comm->getNumProcs() - 1);

	
	//std::cerr << comm->getProcId() << "        Write Buffers";
	// Reserve space in the message for the header
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i == comm->getProcId())
			continue;
		buffer[i].reset();
		buffer[i].advance(sizeof(size_t));
		//buffer[i].advance(1);
	}
	//std::cerr << "\n";

	//std::cerr << comm->getProcId() << "        [ ";
	// Insert Data that dont belong to me in the message
	for ( size_t i = 0; i < size; ++i){
		K & key = keys[i];

		int owner = 1 + hash.get(key);
		
		if (owner == comm->getProcId())
			continue;
		buffer[owner] << key <<  data[i];
		dataSize[owner]++;
		deleted[i] = true;
	}

	//std::cerr << " - ";


	comm->joinSlaves();

	//Ti[0] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	tryShrink = EDBKSendData(comm, dataSize);

	dirty = EDBKRecvData(comm, deleted, pos, tryShrink);

	//Ti[1] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	EDBKShrinkData(deleted, pos, tryShrink);
	//std::cerr << "        (new size: " << localData->getSize() << ")\n";
	
	// Clear Key location saved by last ByKey function
	if ( dirty | tryShrink ){
		if (keyLocations.size() > 0){
			keyLocations.clear();
			//std::cerr << " CLEAR KeyLocations\n" ;
		}
		dirty = true;
	}
	//Ti[2] = duration_cast<milliseconds>(system_clock::now() - start).count();
	
	//std::cerr << "    \n";
	//std::cerr << " \033[0;33mEDBK\033[0m IDOwner:" << Ti[0] << " ED:" << Ti[1] << " (" << sum(dataSize) << ") Sh:" << Ti[3] << "\n";
	
	//groupByKeyHashed = true;

	return dirty;
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::exchangeDataByKey(fastComm *comm){

	if (keyMap.use_count() > 0)
		exchangeDataByKeyMapped(comm);
	else
		exchangeDataByKeyHashed(comm);

}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::findMyKeys(int numProcs, int id){
	uKeys = std::make_shared<std::vector<K>>();
	uKeys->reserve( keyMap->size() / std::max(1, numProcs - 2) );

	for( auto it = keyMap->begin(); it != keyMap->end(); it++ ){
			if ( it->second == id ){
				//std::cerr << "\033[0;31m";
				uKeys->insert(uKeys->end(), it->first);
			}
			//std::cerr  << it->first << "\033[0m ";
	}
}


template <typename K, typename T>
void faster::workerIFddCore<K,T>::findMyKeysByHash(int numProcs){
	K * keys = localData->getKeys();
	size_t size = localData->getSize();
	std::unordered_map<K, bool> h(size / std::max(1, numProcs - 2));
	int i = 0;
	
	h.reserve( size / std::max(1, numProcs - 2) );
	//uKeys->reserve( size / std::max(1, numProcs - 2) );

	for ( size_t i = 0; i < size; ++i){
		auto loc = h.find(keys[i]);

		if (loc == h.end())
			h.insert(h.end(), std::make_pair(keys[i], true));
	}
	
	uKeys = std::make_shared<std::vector<K>>(h.size());

	for ( auto it = h.begin(); it != h.end(); it++ ){
		(*uKeys)[i++] = it->first;
	}

}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::groupByKey(fastComm *comm){
	//size_t numMyKeys = 0;
	unsigned long tid = 0;
	fastCommBuffer &resultBuffer = comm->getResultBuffer();
	//std::cerr << "      groupByKey\n";
	
	//using std::chrono::system_clock;
	//using std::chrono::duration_cast;
	//using std::chrono::milliseconds;
	//int Ti[5];
	//auto start = system_clock::now();

	//std::cerr << "      RecvKeyMap\n";
	keyMap = std::make_shared<std::unordered_map<K, int>>();
	comm->recvKeyMap(tid, *keyMap);

	// Find out how many keys I own
	//for ( auto it = keyMap->begin(); it != keyMap->end(); it++)
		//if (it->second == comm->getProcId())
			//numMyKeys++;
	//localData->setNumKeys(numMyKeys);
	//std::cerr << "      NumKeys: " << numMyKeys << "\n";
	//Ti[0] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	findMyKeys(comm->getNumProcs(), comm->getProcId());
	//Ti[1] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	exchangeDataByKeyMapped(comm);
	//Ti[2] = duration_cast<milliseconds>(system_clock::now() - start).count();
	
	//comm->waitForReq(comm->getNumProcs()-1);
	resultBuffer << size_t(localData->getSize());

	//std::cerr << "    DONE rKM:" << Ti[0] << " fKM:" << Ti[1] << " eDBK:" << Ti[2]  << "\n";
}
template <typename K, typename T>
void faster::workerIFddCore<K,T>::groupByKeyHashed(fastComm *comm){
	fastCommBuffer &resultBuffer = comm->getResultBuffer();
	//std::cerr << "      groupByKeyHashed\n";

	bool dirty = exchangeDataByKeyHashed(comm);

	if (dirty)
		findMyKeysByHash(comm->getNumProcs());


	resultBuffer << size_t(localData->getSize());
}

inline void parseData(std::string & item, std::string & d){
	d = item;
}
inline void parseData(std::string & item, double & d){
	d = std::atof(item.c_str());
}
inline void parseData(std::string & item, float & d){
	d = std::atof(item.c_str());
}
inline void parseData(std::string & item, long int & d){
	d = std::atol(item.c_str());
}
inline void parseData(std::string & item, int & d){
	d = std::atoi(item.c_str());
}
inline void parseData(std::string & item, char & d){
	d = item[0];
}

template <typename T>
inline void parseData(std::stringstream & ss, T & d){
	std::string item; 
	while(std::getline(ss, item, ' ')){
		if(item.size() > 0){
			parseData(item, d);
			break;
		}
	}
}

template <typename T>
inline void parseData(std::stringstream & ss, std::vector<T> & vec){
	vec.clear();
	std::string item; 
	T d;

	while(std::getline(ss, item, ' ')){
		if(item.size() > 0){
			parseData(item, d);
			vec.insert(vec.end(), d);
		}
	}
}

template <typename T>
bool waitForLastStage(std::deque<T> & q, bool lastStageDone){
	while(q.size() == 0){
		if (lastStageDone){
			return true;
		}else{
			usleep(10);
		}
	}
	return false;
}
bool onlineReadStage1Full(std::ifstream & inFile, std::deque<std::vector<std::string>> & q1, omp_lock_t & q1lock, const int blocksize){
	std::vector<std::string> lines(blocksize, ""); 

	for ( int i = 0; i < blocksize; i++){
		if( inFile.good() ){
			std::getline( inFile, lines[i] ); 
		}else{
			break;
		}
	}

	omp_set_lock(&q1lock);
	q1.push_back(std::move(lines));
	omp_unset_lock(&q1lock);

	if ( ! inFile.good() ){
		std::cerr << "\033[0;31mF\033[0m";
		return true;
	}

	while(q1.size() >= 100)
		usleep(10);

	return false;
}
bool onlineReadStage1(std::ifstream & inFile, std::deque<std::vector<std::string>> & q1, omp_lock_t & q1lock, size_t endOffset, const int blocksize){
	std::vector<std::string> lines(blocksize, ""); 

	for ( int i = 0; i < blocksize; i++){
		if( inFile.good()  && ( size_t(inFile.tellg()) <= endOffset ) ){
			std::getline( inFile, lines[i] ); 
		}else{
			break;
		}
	}

	omp_set_lock(&q1lock);
	q1.push_back(std::move(lines));
	omp_unset_lock(&q1lock);

	if ( (! inFile.good()) || ( size_t(inFile.tellg()) > endOffset ) ) 
		return true;

	while(q1.size() >= 100)
		usleep(10);

	return false;
}
template <typename K, typename T>
bool onlineReadStage2(std::deque<std::vector<std::string>> & q1, omp_lock_t & q1lock, std::deque<std::vector<std::pair<K,T>>> & q2, omp_lock_t & q2lock){
	std::vector<std::string> lines; 
	static std::stringstream ss;
	std::pair<K,T> item;

	omp_set_lock(&q1lock);
	lines = std::move(q1.front());
	q1.pop_front();
	omp_unset_lock(&q1lock);

	std::vector<std::pair<K,T>> items;
	items.reserve(lines.size());

	omp_set_num_threads(10);
	#pragma omp parallel for schedule(dynamic,10) private(ss)
	for ( size_t i = 0; i < lines.size(); i++){
		if (lines[i].length() > 0 ){
			//std::cerr << lines[i] << "\n";
			ss.clear();
			ss.str(lines[i]);		
			parseData(ss, item.first);
			parseData(ss, item.second);
			items.insert(items.end(), item);
		}
	}

	omp_set_lock(&q2lock);
	q2.push_back(std::move(items));
	omp_unset_lock(&q2lock);

	while(q2.size() >= 100)
		usleep(10);

	return false;
}
template <typename K, typename T>
bool faster::workerIFddCore<K,T>::onlineReadStage3(std::unordered_map<K, int> & localKeyMap, fastComm *comm, void * funcP, std::deque<std::vector<std::pair<K,T>>> & q2, omp_lock_t & q2lock){
	std::vector<std::pair<K,T>> items;

	omp_set_lock(&q2lock);
	items = std::move(q2.front());
	q2.pop_front();
	omp_unset_lock(&q2lock);

	for ( size_t i = 0; i < items.size(); i++){
		K & key = items[i].first;
		T & data = items[i].second;

		int myPart = ( (IonlineFullPartFuncP<K,T>) funcP ) ( key, data ) ;
		if ( myPart == comm->getProcId() ) {
			this->localData->insert(key, data);
		}

		auto location = localKeyMap.find(key);
		if (location == localKeyMap.end()){
			uKeys->insert(uKeys->end(), key);
			localKeyMap.insert(std::make_pair(key, myPart));
		}else{
			location->second =  myPart;
		}
	}
	return false;
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::onlineFullPartRead(fastComm *comm, void * funcP){
	std::string filename;
	keyMap = std::make_shared<std::unordered_map<K, int>>();
	uKeys = std::make_shared<std::vector<K>>();

	// Get file path
	comm->recvFileName(filename);

	// Open File
	std::ifstream inFile(filename, std::ifstream::in);
	
	// Start reading lines
	/*std::string line; 
	while( inFile.good() ){
		K key;
		T d;
		std::getline( inFile, line ); 

		if (line.length() == 0) 
			continue;

		ss.clear();
		ss.str(line);		
		parseData(ss, key);
		parseData(ss, d);

		int myPart = ( (IonlineFullPartFuncP<K,T>) funcP ) ( key, d) ;
		if ( myPart == comm->getProcId() ) {
			this->localData->insert(key, d);
		}

		auto location = keyMap->find(key);
		if (location == keyMap->end()){
			uKeys->insert(uKeys->end(), key);
			keyMap->insert(std::make_pair(key, myPart));
		}else{
			location->second =  myPart;
		}


	}// */

	std::deque<std::vector<std::string>> q1;
	std::deque<std::vector<std::pair<K,T>>> q2;
	omp_lock_t q1lock;
	omp_lock_t q2lock;
	omp_init_lock(&q1lock);
	omp_init_lock(&q2lock);
	bool stage1Done = false;
	bool stage2Done = false;
	bool stage3Done = false;
	const int blocksize = 1000;
	#pragma omp parallel
	{
		#pragma omp sections
		{
			// Read fron disk
			#pragma omp section
			while(  ! stage1Done ){
				std::cerr << 'R';
				stage1Done = onlineReadStage1Full(inFile, q1, q1lock, blocksize);
			}

			// Parse lines
			#pragma omp section
			while( ! stage2Done ){
				std::cerr << 'P';

				stage2Done = waitForLastStage(q1, stage1Done);
				if ( stage2Done ) break;
				
				stage2Done = onlineReadStage2(q1, q1lock, q2, q2lock);

			}

			// Insert items into the dataset
			#pragma omp section
			while( ! stage3Done ){
				std::cerr << 'I';

				stage3Done = waitForLastStage(q2, stage2Done);
				if ( stage3Done ) break;

				stage2Done = onlineReadStage3(*this->keyMap, comm, funcP, q2, q2lock);
			}
		}
	}
				std::cerr << " DONE\n";
	omp_destroy_lock(&q1lock);
	omp_destroy_lock(&q2lock);// */
}

void findFileOffset(fastComm *comm, std::ifstream & inFile, size_t & endOffset, std::deque<std::vector<std::string>> & q1){
	size_t offset, size;
	std::vector<std::string> line(1); 

	inFile.seekg(0, std::ifstream::end);
	size = inFile.tellg() / (comm->getNumProcs() - 1);
	offset = (comm->getProcId() - 1) * size;
	endOffset = offset + size;

	inFile.seekg(offset, inFile.beg);

	if ( offset > 0 ){
		char c = inFile.get();
		std::getline( inFile, line[0] ); 
		if ( c == '\n' ){
			q1.push_back(std::move(line));
		}

	}
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::onlinePartRead(fastComm *comm, void * funcP){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	auto start = system_clock::now();
	std::string filename;
	std::deque<std::vector<std::string>> q1;
	std::deque<std::vector<std::pair<K,T>>> q2;
	omp_lock_t q1lock;
	omp_lock_t q2lock;
	omp_init_lock(&q1lock);
	omp_init_lock(&q2lock);
	bool stage1Done = false;
	bool stage2Done = false;
	bool stage3Done = false;
	const int blocksize = 200;
	size_t endOffset;

	// Init
	keyMap = std::make_shared<std::unordered_map<K, int>>();
	std::unordered_map<K, int> localKeyMap;
	uKeys = std::make_shared<std::vector<K>>();

	// Get file path
	comm->recvFileName(filename);

	// Open File
	std::ifstream inFile(filename, std::ifstream::in);

	// Find file offset
	findFileOffset(comm, inFile, endOffset, q1);

	#pragma omp parallel
	{
		#pragma omp sections
		{
			// Read fron disk
			#pragma omp section
			while(  ! stage1Done ){
				stage1Done = onlineReadStage1(inFile, q1, q1lock, endOffset, blocksize);
			}

			// Parse line
			#pragma omp section
			while( ! stage2Done ){

				stage2Done = waitForLastStage(q1, stage1Done);
				if ( stage2Done ) break;
				
				stage2Done = onlineReadStage2(q1, q1lock, q2, q2lock);

			}

			#pragma omp section
			while( ! stage3Done ){

				stage3Done = waitForLastStage(q2, stage2Done);
				if ( stage3Done ) break;

				stage3Done = onlineReadStage3(localKeyMap, comm, funcP, q2, q2lock);
			}
		}
	}
	omp_destroy_lock(&q1lock);
	omp_destroy_lock(&q2lock);// */

	std::cerr << " Read:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
	start = system_clock::now();

	comm->distributeKeyMap(localKeyMap, *keyMap);
	std::cerr << " DistKeys:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
}

template <typename K, typename T>
void printData(std::ofstream & outFile, K * keys, std::vector<T> * data, size_t s){
	outFile.precision(10);
	for ( size_t i = 0; i < s; i++){
		for ( size_t j = 0; j < data[i].size(); j++){
			outFile << keys[i] << " " << data[i][j] << " ";
		}
		outFile	<< "\n";
	}
}

template <typename K, typename T>
void printData(std::ofstream & outFile, K * keys, T * data, size_t s){
	outFile.precision(10);
	for ( size_t i = 0; i < s; i++){
		outFile << keys[i] << " " << data[i] << "\n";
	}
}

template <typename K, typename T>
void faster::workerIFddCore<K, T>::writeToFile(void * pathP, size_t procId, void * sufixP){
	std::string path = * (std::string*) pathP;
	std::string sufix = * (std::string*) sufixP;

	K * keys = localData->getKeys();
	T * data = localData->getData();
	size_t s = localData->getSize();

	std::string filename(path + std::to_string(procId) + sufix);

	std::ofstream outFile(filename, std::ofstream::out);

	printData(outFile, keys, data, s);
}

template <typename K, typename T>
void faster::workerIFddCore<K, T>::preapply(long unsigned int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){ 
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	auto start = system_clock::now();
	fastCommBuffer &buffer = comm->getResultBuffer();
	size_t durationP;
	size_t rSizeP;
	size_t rStatP;
	size_t headerSize;
	char ret = 0;
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

	if (op & (OP_GENERICMAP | OP_GENERICREDUCE)){
		this->apply(func, op, dest, buffer);
		if (dest) buffer << size_t(dest->getSize());
	}else{
		switch(op){
			case OP_CountByKey:
				countByKey(comm);
				break;
			case OP_GroupByKey:
				groupByKey(comm);
				break;
			case OP_GroupByKeyH:
				groupByKeyHashed(comm);
				break;
			case OP_OnPartRead:
				onlinePartRead(comm, func);
				buffer << size_t(this->getSize());
				break;
			case OP_OnFullPRead:
				onlineFullPartRead(comm, func);
				buffer << size_t(this->getSize());
				break;
		}
		buffer << ret;
	}
	auto end = system_clock::now();
	auto duration = duration_cast<milliseconds>(end - start);
	//std::cerr << " ET:" << duration.count() << " ";

	buffer.writePos(size_t(duration.count()), durationP);
	buffer.writePos(getProcStat(), rStatP);
	buffer.writePos(size_t(buffer.size() - headerSize), rSizeP);

	comm->sendTaskResult();

}


template class faster::workerIFddCore<char, char>;
template class faster::workerIFddCore<char, int>;
template class faster::workerIFddCore<char, long int>;
template class faster::workerIFddCore<char, float>;
template class faster::workerIFddCore<char, double>;
//template class faster::workerIFddCore<char, char*>;
//template class faster::workerIFddCore<char, int*>;
//template class faster::workerIFddCore<char, long int*>;
//template class faster::workerIFddCore<char, float*>;
//template class faster::workerIFddCore<char, double*>;
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
//template class faster::workerIFddCore<int, char*>;
//template class faster::workerIFddCore<int, int*>;
//template class faster::workerIFddCore<int, long int*>;
//template class faster::workerIFddCore<int, float*>;
//template class faster::workerIFddCore<int, double*>;
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
//template class faster::workerIFddCore<long int, char*>;
//template class faster::workerIFddCore<long int, int*>;
//template class faster::workerIFddCore<long int, long int*>;
//template class faster::workerIFddCore<long int, float*>;
//template class faster::workerIFddCore<long int, double*>;
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
//template class faster::workerIFddCore<float, char*>;
//template class faster::workerIFddCore<float, int*>;
//template class faster::workerIFddCore<float, long int*>;
//template class faster::workerIFddCore<float, float*>;
//template class faster::workerIFddCore<float, double*>;
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
//template class faster::workerIFddCore<double, char*>;
//template class faster::workerIFddCore<double, int*>;
//template class faster::workerIFddCore<double, long int*>;
//template class faster::workerIFddCore<double, float*>;
//template class faster::workerIFddCore<double, double*>;
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
//template class faster::workerIFddCore<std::string, char*>;
//template class faster::workerIFddCore<std::string, int*>;
//template class faster::workerIFddCore<std::string, long int*>;
//template class faster::workerIFddCore<std::string, float*>;
//template class faster::workerIFddCore<std::string, double*>;
template class faster::workerIFddCore<std::string, std::string>;
template class faster::workerIFddCore<std::string, std::vector<char>>;
template class faster::workerIFddCore<std::string, std::vector<int>>;
template class faster::workerIFddCore<std::string, std::vector<long int>>;
template class faster::workerIFddCore<std::string, std::vector<float>>;
template class faster::workerIFddCore<std::string, std::vector<double>>;

