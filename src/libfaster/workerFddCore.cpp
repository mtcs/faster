#include "workerFdd.h"
#include "fastCommBuffer.h"
#include "fddStorageExtern.cpp"

template <typename T>
workerFddCore<T>::workerFddCore(unsigned int ident, fddType t) : workerFddBase(ident, t){
	localData = new fddStorage<T>();
} 

template <typename T>
workerFddCore<T>::workerFddCore(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
	localData = new fddStorage<T>(size);
}

template <typename T>
workerFddCore<T>::~workerFddCore(){
	delete resultBuffer;
	delete localData;
}

template <typename T>
fddType workerFddCore<T>::getType()  { 
	return type; 
}
template <typename T>
fddType workerFddCore<T>::getKeyType()  { 
	return Null; 
}

template <typename T>
T & workerFddCore<T>::operator[](size_t address){ 
	return localData->getData()[address]; 
}
template <typename T>
void * workerFddCore<T>::getData() { 
	return localData->getData(); 
}
template <typename T>
size_t workerFddCore<T>::getSize() { 
	return localData->getSize(); 
}
template <typename T>
size_t workerFddCore<T>::itemSize() { 
	return sizeof(T); 
}
template <typename T>
size_t workerFddCore<T>::baseSize() { 
	return sizeof(T); 
}
template <typename T>
void workerFddCore<T>::deleteItem(void * item)  { 
	delete (T*) item; 
}
template <typename T>
void workerFddCore<T>::shrink(){ 
	localData->shrink(); 
}


template class workerFddCore<char>;
template class workerFddCore<int>;
template class workerFddCore<long int>;
template class workerFddCore<float>;
template class workerFddCore<double>;

template class workerFddCore<char*>;
template class workerFddCore<int*>;
template class workerFddCore<long int*>;
template class workerFddCore<float*>;
template class workerFddCore<double*>;

template class workerFddCore<std::string>;

template class workerFddCore<std::vector<char>>;
template class workerFddCore<std::vector<int>>;
template class workerFddCore<std::vector<long int>>;
template class workerFddCore<std::vector<float>>;
template class workerFddCore<std::vector<double>>;
