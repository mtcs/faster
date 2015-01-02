#include <chrono>
#include <iostream>

#include "_workerIFdd.h"
#include "fastComm.h"
#include "misc.h"
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
void faster::workerIFddCore<K,T>::exchangeDataByKey(fastComm *comm, void * keyMapP){
	//using std::chrono::system_clock;
	//using std::chrono::duration_cast;
	//using std::chrono::milliseconds;

	//std::cerr << "    \033[0;33mExchange Data By Key\033[0m\n";
	K * keys = localData->getKeys();
	T * data = localData->getData();
	size_t size = localData->getSize();
	std::unordered_map<K, int> keyMap = * (std::unordered_map<K, int> *)keyMapP;
	fastCommBuffer * buffer = comm->getSendBuffers();
	std::vector<size_t> dataSize(comm->getNumProcs(), 0);
	std::vector<bool> deleted(size, false);
	size_t pos;
	bool dirty = false;
	bool tryShrink = false;

	//int Ti[5];
	//auto start = system_clock::now();
	
	//std::cerr << "\033[0;33mPRESS ENTER TO CONTINUE\033[0m\n";std::cin.get();
	//std::cerr << comm->getProcId() << "        [ KeyMap: ";
	if ( uKeys.size() != 0 ){
		uKeys.clear();
		uKeys.reserve(keyMap.size()/2);
	}
	for ( auto it = keyMap.begin(); it != keyMap.end(); it++){
		if (it->second == comm->getProcId()){
			uKeys.insert(uKeys.end(), it->first);
			//std::cerr << "\033[0;33m";
		}
		//std::cerr << it->first << "\033[0m ";
	}
	//std::cerr << "] \n";

	//std::cerr << comm->getProcId() << "        Write Buffers";
	// Reserve space in the message for the header
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		size_t numSend = 0;
		if (i == comm->getProcId())
			continue;
		buffer[i].reset();
		buffer[i] << numSend ;
	}
	//std::cerr << "\n";

	//std::cerr << comm->getProcId() << "        [ ";
	// Insert Data that dont belong to me in the message
	for ( size_t i = 0; i < size; ++i){
		K key = keys[i];
		int owner = keyMap[key];
		
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

	comm->join();

	//Ti[0] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	// Include the data size in the message header
	for ( int i = 1; i < (comm->getNumProcs()); ++i){
		if (i == comm->getProcId())
			continue;
		if (dataSize[i] > 0)
			tryShrink = true;

		buffer[i].writePos(dataSize[i], 0);
		comm->sendGroupByKeyData(i);
		//std::cerr << "\033[0;31m"<< dataSize[i] << " \033[0m> " << i << "  ";
	}
	//std::cerr << "]\n";

	//Ti[1] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	//std::cerr << comm->getProcId() << "        Recv data In-place\n";
	//std::cerr << comm->getProcId() << "          Insert: [";
	// Find the first emty space
	pos = 0;
	if (tryShrink){
		while ( (pos < deleted.size() ) && ( ! deleted[pos] ) )
			pos++;
	}else{
		pos = deleted.size();
	}
	
	// Recv all keys I own in-place
	for ( int i = 1; i < (comm->getNumProcs() - 1); ++i){
		int rSize;
		size_t numItems;
		fastCommBuffer rb(0);
		void * rData = comm->recvGroupByKeyData(rSize);
		rb.setBuffer(rData, rSize);

		rb >> numItems;
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
	}
	//std::cerr << "]\n";
	comm->waitForReq(comm->getNumProcs() - 2);

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

	// Clear Key location saved by last ByKey function
	if (dirty | tryShrink){
		if (keyLocations.size() > 0){
			keyLocations.clear();
			//std::cerr << " CLEAR KeyLocations\n" ;
		}
	}
	//std::cerr << "        (new size: " << localData->getSize() << ")\n";
	
	//Ti[3] = duration_cast<milliseconds>(system_clock::now() - start).count();
	//start = system_clock::now();

	//std::cerr << ".\n";
}

template <typename K, typename T>
void faster::workerIFddCore<K,T>::groupByKey(fastComm *comm){
	//size_t numMyKeys = 0;
	unsigned long tid = 0;
	std::unordered_map<K, int> keyMap;
	fastCommBuffer &resultBuffer = comm->getResultBuffer();

	//std::cerr << "      RecvKeyMap\n";
	comm->recvKeyMap(tid, keyMap);

	// Find out how many keys I own
	//for ( auto it = keyMap.begin(); it != keyMap.end(); it++)
		//if (it->second == comm->getProcId())
			//numMyKeys++;
	//localData->setNumKeys(numMyKeys);
	//std::cerr << "      NumKeys: " << numMyKeys << "\n";

	exchangeDataByKey(comm, &keyMap);
	
	//comm->waitForReq(comm->getNumProcs()-1);
	resultBuffer << size_t(localData->getSize());

	//std::cerr << "    DONE\n";
}


template <typename K, typename T>
void faster::workerIFddCore<K, T>::preapply(long unsigned int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){ 
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

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

	rSizeP = buffer.size();
	buffer.advance(sizeof(size_t));

	rStatP = buffer.size();
	buffer.advance(s);

	headerSize = buffer.size();

	auto start = system_clock::now();
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
		}
		buffer << ret;
	}
	auto end = system_clock::now();
	auto duration = duration_cast<milliseconds>(end - start);
	//std::cerr << " ET:" << duration.count() << " ";

	buffer.writePos(size_t(duration.count()), durationP);
	buffer.writePos(size_t(buffer.size() - headerSize), rSizeP);
	buffer.writePos(getProcStat(), rStatP);

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

