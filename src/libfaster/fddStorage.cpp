#include <string>
#include <iostream>

#include "fastCommBuffer.h"
#include "fddStorage.h"

template <class T> 
faster::fddStorageCore<T>::fddStorageCore(){
	allocSize = 1000;
	localData = new T[allocSize];
	size = 0;
}

template <class T> 
faster::fddStorageCore<T>::fddStorageCore(size_t s){
	if (s > 0)
		allocSize = s;
	else
		allocSize = 1000;
	localData = new T[allocSize];
	size = s;
}
template <class T> 
faster::fddStorageCore<T>::~fddStorageCore(){
	delete [] localData;
}

template <class T> 
T * faster::fddStorageCore<T>::getData(){ 
	return localData; 
}

template <class T> 
T & faster::fddStorageCore<T>::operator[](size_t ref){ 
	return localData[ref]; 
}




template class faster::fddStorageCore<char>;
template class faster::fddStorageCore<int>;
template class faster::fddStorageCore<long int>;
template class faster::fddStorageCore<float>;
template class faster::fddStorageCore<double>;
template class faster::fddStorageCore<char *>;
template class faster::fddStorageCore<int *>;
template class faster::fddStorageCore<long int *>;
template class faster::fddStorageCore<float *>;
template class faster::fddStorageCore<double *>;
template class faster::fddStorageCore<std::string>;
template class faster::fddStorageCore<std::vector<char>>;
template class faster::fddStorageCore<std::vector<int>>;
template class faster::fddStorageCore<std::vector<long int>>;
template class faster::fddStorageCore<std::vector<float>>;
template class faster::fddStorageCore<std::vector<double>>;




template <class T> 
faster::fddStorage<T>::fddStorage() : fddStorageCore<T>(){}
template <class T> 
faster::fddStorage<T*>::fddStorage() : fddStorageCore<T*>(){
}

template <class T> 
faster::fddStorage<T*>::fddStorage(size_t s):fddStorageCore<T *>(s){
	lineSizes = new size_t[s];
}

template <class T> 
faster::fddStorage<T>::fddStorage(T * data, size_t s) : fddStorage<T>(s){
	setData(data, s);
}

template <class T> 
faster::fddStorage<T*>::fddStorage(T ** data, size_t * lineSizes, size_t s) : fddStorage(s){
	setData( data, lineSizes, s);
}




template <class T> 
faster::fddStorage<T*>::~fddStorage(){
	if (lineSizes != NULL){
		delete [] lineSizes;
	}
}		




template <class T> 
void faster::fddStorage<T>::setData( T * data, size_t s){
	grow(s);
	this->size = s;

	for ( size_t i = 0; i < s; ++i){
		this->localData[i] = data[i];
	}
}
// Works for primitive and Containers

template <class T> 
void faster::fddStorage<T*>::setData( T ** data, size_t * ls, size_t s){
	grow(s);
	#pragma omp parallel for
	for ( size_t i = 0; i < s; ++i){
		lineSizes[i] = ls[i];
		
		this->localData[i] = new  T [lineSizes[i]];
		for ( size_t j = 0; j < lineSizes[i]; ++j){
			this->localData[i][j] =  data[i][j];
		}
	}
	this->size = s;
}
template <class T> 
void faster::fddStorage<T>::setDataRaw(void * data, size_t s){
	fastCommBuffer buffer(0);

	//grow(s);
	//this->size = s;
	buffer.setBuffer(data, s);

	//std::cerr << "\nfddStorage setData ";
	for ( size_t i = 0; i < this->size; ++i){
		//std::cerr << ((int *) data)[i] << " ";
		buffer >> this->localData[i];
	}
	//std::cerr << "\n";
}
template <class T> 
void faster::fddStorage<T*>::setDataRaw( void * data, size_t * ls, size_t s){
	fastCommBuffer buffer(0);

	buffer.setBuffer(data, s);

	for ( size_t i = 0; i < s; ++i){
		lineSizes[i] = ls[i];

		this->localData[i] = new  T [lineSizes[i]];

		buffer.read(this->localData[i], ls[i]*sizeof(T));
	}
}

template <class T> 
void   faster::fddStorage<T>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}
template <class T> 
void   faster::fddStorage<T*>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}

template <class T> 
void faster::fddStorage<T>::insert(T & item){
	grow(this->size + 1);
	this->localData[this->size++] = item;	
}

template <class T> 
void faster::fddStorage<T*>::insert(T *& item, size_t s){
	grow(this->size + 1);
	lineSizes[this->size] = s;	
	this->localData[this->size++] = item;	

}






template <class T> 
size_t * faster::fddStorage<T*>::getLineSizes(){ 
	return lineSizes; 
}




template <class T> 
void faster::fddStorage<T>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		T * newStorage = new T [toSize];

		if (this->size >0) 
			#pragma omp parallel for
			for ( size_t i = 0; i < this->size; ++i)
				newStorage[i] = this->localData[i];
			//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;

		this->localData = newStorage;
		this->allocSize = toSize;
	}
}
template <class T> 
void faster::fddStorage<T*>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		size_t * newLineSizes = new size_t [toSize];
		T ** newStorage = new T* [toSize];

		if (this->size > 0){
			//memcpy(newStorage, localData, this->size * sizeof( T* ) );
			//memcpy(newLineSizes, lineSizes, this->size * sizeof( size_t ) );
			#pragma omp parallel for
			for ( size_t i = 0; i < this->size; ++i){
				newStorage[i] =  this->localData[i];
				newLineSizes[i] = lineSizes[i];
			}
		}

		delete [] this->localData;
		delete [] lineSizes;

		this->localData = newStorage;
		lineSizes = newLineSizes;
		this->allocSize = toSize;

	}
}






template <class T> 
void faster::fddStorage<T>::shrink(){
	if ( (this->size > 0) && (this->allocSize > this->size) ){
		T * newStorage = new T [this->size];

		for ( size_t i = 0; i < this->size; ++i)
			newStorage[i] = this->localData[i];

		delete [] this->localData;

		this->localData = newStorage;
		this->allocSize = this->size;
	}
}
template <class T> 
void faster::fddStorage<T*>::shrink(){
	if ( (this->size > 0) && (this->allocSize > this->size) ){
		T ** newStorage = new T* [this->size];
		size_t * newLineSizes = new size_t[this->size];

		for ( size_t i = 0; i < this->size; ++i){
			newStorage[i] = this->localData[i];
			newLineSizes[i] = lineSizes[i];
		}
		//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;
		delete [] lineSizes;

		this->localData = newStorage;
		lineSizes = newLineSizes;
		this->allocSize = this->size;
	}
}




template class faster::fddStorage<char>;
template class faster::fddStorage<int>;
template class faster::fddStorage<long int>;
template class faster::fddStorage<float>;
template class faster::fddStorage<double>;
template class faster::fddStorage<char *>;
template class faster::fddStorage<int *>;
template class faster::fddStorage<long int *>;
template class faster::fddStorage<float *>;
template class faster::fddStorage<double *>;
template class faster::fddStorage<std::string>;
template class faster::fddStorage<std::vector<char>>;
template class faster::fddStorage<std::vector<int>>;
template class faster::fddStorage<std::vector<long int>>;
template class faster::fddStorage<std::vector<float>>;
template class faster::fddStorage<std::vector<double>>;
