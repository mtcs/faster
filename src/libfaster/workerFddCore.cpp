#include <fstream>
#include <chrono>
#include <iomanip>

#include "_workerFdd.h"
#include "fastComm.h"
#include "fastCommBuffer.h"
#include "fddStorageExtern.cpp"
#include "misc.h"


template <typename T>
faster::workerFddCore<T>::workerFddCore(unsigned int ident, fddType t) : workerFddBase(ident, t){
	keyType = Null;
	localData = new fddStorage<T>();
} 

template <typename T>
faster::workerFddCore<T>::workerFddCore(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
	keyType = Null;
	if (size == 0)
		localData = new fddStorage<T>();
	else
		localData = new fddStorage<T>(size);
}

template <typename T>
faster::workerFddCore<T>::~workerFddCore(){
	delete localData;
}

template <typename T>
faster::fddType faster::workerFddCore<T>::getType()  { 
	return type; 
}
template <typename T>
faster::fddType faster::workerFddCore<T>::getKeyType()  { 
	return Null; 
}

template <typename T>
T & faster::workerFddCore<T>::operator[](size_t address){ 
	return localData->getData()[address]; 
}
template <typename T>
void * faster::workerFddCore<T>::getData() { 
	return localData->getData(); 
}
template <typename T>
size_t faster::workerFddCore<T>::getSize() { 
	return localData->getSize(); 
}
template <typename T>
size_t faster::workerFddCore<T>::itemSize() { 
	return sizeof(T); 
}
template <typename T>
size_t faster::workerFddCore<T>::baseSize() { 
	return sizeof(T); 
}
template <typename T>
void faster::workerFddCore<T>::setSize(size_t s) { 
	localData->setSize(s); 
}

template <typename T>
void faster::workerFddCore<T>::deleteItem(void * item)  { 
	delete (T*) item; 
}
template <typename T>
void faster::workerFddCore<T>::shrink(){ 
	localData->shrink(); 
}

template <typename T>
void printData(std::ofstream & outFile, std::vector<T> * data, size_t s){
	outFile.precision(10);
	for ( size_t i = 0; i < s; i++){
		for ( size_t j = 0; j < data[i].size(); j++){
			outFile << data[i][j] << " ";
		}
		outFile	<< "\n";
	}
}

template <typename T>
void printData(std::ofstream & outFile, T * data, size_t s){
	outFile.precision(10);
	for ( size_t i = 0; i < s; i++){
		outFile << data[i] << "\n";
	}
}

template <typename T>
void faster::workerFddCore<T>::writeToFile(void * pathP, size_t procId, void * sufixP){
	std::string path = * (std::string*) pathP;
	std::string sufix = * (std::string*) sufixP;

	T * data = localData->getData();
	size_t s = localData->getSize();

	std::string filename(path + std::to_string(procId) + sufix);
	std::ofstream outFile(filename, std::ofstream::out);

	printData(outFile, data, s);

}


template <typename T>
void faster::workerFddCore<T>::preapply(long unsigned int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){ 
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;


	fastCommBuffer &buffer = comm->getResultBuffer();
	size_t durationP;
	size_t rSizeP;
	size_t rStatP;
	size_t headerSize;
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
	if (op & (OP_GENERICMAP | OP_GENERICREDUCE)){
		this->apply(func, op, dest, buffer);

		//if (dest) buffer << size_t(localData->getSize());
		if (dest) buffer << size_t(dest->getSize());
	}
	auto end = system_clock::now();
	auto duration = duration_cast<milliseconds>(end - start);
	//std::cerr << " ET:" << duration.count() << " ";

	buffer.writePos(size_t(duration.count()), durationP);
	buffer.writePos(getProcStat(), rStatP);
	buffer.writePos(size_t(buffer.size() - headerSize), rSizeP);

	comm->sendTaskResult();

}


template class faster::workerFddCore<char>;
template class faster::workerFddCore<int>;
template class faster::workerFddCore<long int>;
template class faster::workerFddCore<float>;
template class faster::workerFddCore<double>;

//template class faster::workerFddCore<char*>;
//template class faster::workerFddCore<int*>;
//template class faster::workerFddCore<long int*>;
//template class faster::workerFddCore<float*>;
//template class faster::workerFddCore<double*>;

template class faster::workerFddCore<std::string>;

template class faster::workerFddCore<std::vector<char>>;
template class faster::workerFddCore<std::vector<int>>;
template class faster::workerFddCore<std::vector<long int>>;
template class faster::workerFddCore<std::vector<float>>;
template class faster::workerFddCore<std::vector<double>>; 

