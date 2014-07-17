#include <string>
#include <iostream>

#include "fastCommBuffer.h"
#include "fddStorage.h"

template <class T> 
fddStorageCore<T>::fddStorageCore(){
	allocSize = 200;
	localData = new T[allocSize];
	size = 0;
}

template <class T> 
fddStorageCore<T>::fddStorageCore(size_t s){
	allocSize = s;
	localData = new T[s];
	size = s;
}
template <class T> 
fddStorageCore<T>::~fddStorageCore(){
	if (localData != NULL){
		delete [] localData;
	}
}

template <class T> 
T * fddStorageCore<T>::getData(){ 
	return localData; 
}

template <class T> 
T & fddStorageCore<T>::operator[](size_t ref){ 
	return localData[ref]; 
}




template class fddStorageCore<char>;
template class fddStorageCore<int>;
template class fddStorageCore<long int>;
template class fddStorageCore<float>;
template class fddStorageCore<double>;
template class fddStorageCore<char *>;
template class fddStorageCore<int *>;
template class fddStorageCore<long int *>;
template class fddStorageCore<float *>;
template class fddStorageCore<double *>;
template class fddStorageCore<std::string>;
template class fddStorageCore<std::vector<char>>;
template class fddStorageCore<std::vector<int>>;
template class fddStorageCore<std::vector<long int>>;
template class fddStorageCore<std::vector<float>>;
template class fddStorageCore<std::vector<double>>;




template <class T> 
fddStorage<T>::fddStorage() : fddStorageCore<T>(){}
template <class T> 
fddStorage<T*>::fddStorage() : fddStorageCore<T*>(){
}

template <class T> 
fddStorage<T *>::fddStorage(size_t s):fddStorageCore<T *>(s){
	lineSizes = new size_t[s];
}

template <class T> 
fddStorage<T>::fddStorage(T * data, size_t s) : fddStorageCore<T>(s){
	setData(data, s);
}

template <class T> 
fddStorage<T *>::fddStorage(T ** data, size_t * lineSizes, size_t s) : fddStorage(s){
	setData( data, lineSizes, s);
}




template <class T> 
fddStorage<T *>::~fddStorage(){
	if (lineSizes != NULL){
		delete [] lineSizes;
	}
}		




template <class T> 
void fddStorage<T>::setData( T * data, size_t s){
	grow(s);
	this->size = s;

	for ( size_t i = 0; i < s; ++i){
		this->localData[i] = data[i];
	}
}
// Works for primitive and Containers
template <class T> 
void fddStorage<T>::setDataRaw(void * data, size_t s){
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
void fddStorage<T *>::setData( T ** data, size_t * ls, size_t s){
	grow(s);
	#pragma omp parallel for
	for ( int i = 0; i < s; ++i){
		lineSizes[i] = ls[i];
		
		this->localData[i] = new  T [lineSizes[i]];
		for ( int j = 0; j < lineSizes[i]; ++j){
			this->localData[i][j] =  data[i][j];
		}
	}
	this->size = s;
}
template <class T> 
void fddStorage<T *>::setDataRaw( void ** data, size_t * ls, size_t s){
	setData( (T**) data, ls, s );
}

template <class T> 
void   fddStorage<T>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}
template <class T> 
void   fddStorage<T*>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}

template <class T> 
void fddStorage<T>::insert(T & item){
	grow(this->size + 1);
	this->localData[this->size++] = item;	
}

template <class T> 
void fddStorage<T *>::insert(T *& item, size_t s){
	grow(this->size + 1);
	lineSizes[this->size] = s;	
	this->localData[this->size++] = item;	

}







template <class T> 
size_t * fddStorage<T *>::getLineSizes(){ 
	return lineSizes; 
}




template <class T> 
void fddStorage<T>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		T * newStorage = new T [toSize];

		if (this->size >0) 
			for ( size_t i = 0; i < this->size; ++i)
				newStorage[i] = this->localData[i];
			//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;

		this->localData = newStorage;
		this->allocSize = toSize;
	}
}
template <class T> 
void fddStorage<T *>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		size_t * newLineSizes = new size_t [toSize];
		T ** newStorage = new T* [toSize];

		if (this->size > 0){
			//memcpy(newStorage, localData, this->size * sizeof( T* ) );
			//memcpy(newLineSizes, lineSizes, this->size * sizeof( size_t ) );
			for ( int i = 0; i < this->size; ++i){
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
void fddStorage<T>::shrink(){
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
void fddStorage<T*>::shrink(){
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




template class fddStorage<char>;
template class fddStorage<int>;
template class fddStorage<long int>;
template class fddStorage<float>;
template class fddStorage<double>;
template class fddStorage<char *>;
template class fddStorage<int *>;
template class fddStorage<long int *>;
template class fddStorage<float *>;
template class fddStorage<double *>;
template class fddStorage<std::string>;
template class fddStorage<std::vector<char>>;
template class fddStorage<std::vector<int>>;
template class fddStorage<std::vector<long int>>;
template class fddStorage<std::vector<float>>;
template class fddStorage<std::vector<double>>;
