#include "workerFdd.h"
#include "fastCommBuffer.h"
#include "fddStorageExtern.cpp"

template <typename T>
faster::workerFddCore<T>::workerFddCore(unsigned int ident, fddType t) : workerFddBase(ident, t){
	localData = new fddStorage<T>();
} 

template <typename T>
faster::workerFddCore<T>::workerFddCore(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
	localData = new fddStorage<T>(size);
}

template <typename T>
faster::workerFddCore<T>::~workerFddCore(){
	delete resultBuffer;
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
void faster::workerFddCore<T>::deleteItem(void * item)  { 
	delete (T*) item; 
}
template <typename T>
void faster::workerFddCore<T>::shrink(){ 
	localData->shrink(); 
}


template class faster::workerFddCore<char>;
template class faster::workerFddCore<int>;
template class faster::workerFddCore<long int>;
template class faster::workerFddCore<float>;
template class faster::workerFddCore<double>;

template class faster::workerFddCore<char*>;
template class faster::workerFddCore<int*>;
template class faster::workerFddCore<long int*>;
template class faster::workerFddCore<float*>;
template class faster::workerFddCore<double*>;

template class faster::workerFddCore<std::string>;

template class faster::workerFddCore<std::vector<char>>;
template class faster::workerFddCore<std::vector<int>>;
template class faster::workerFddCore<std::vector<long int>>;
template class faster::workerFddCore<std::vector<float>>;
template class faster::workerFddCore<std::vector<double>>;
