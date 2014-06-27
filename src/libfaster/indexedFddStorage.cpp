#include "fddStorage.h"

template <class T> 
fddStorage<T>::fddStorage(){
	allocSize = 200;
	localData = new T[allocSize];
	size = 0;
}
template <class T> 
fddStorage<T *>::fddStorage(){
	allocSize = 200;
	localData = new T*[allocSize];
	size = 0;
}
fddStorage<std::string>::fddStorage(){
	allocSize = 200;
	localData = new std::string[allocSize];
	size = 0;
}
fddStorage<void *>::fddStorage(){
	allocSize = 200;
	localData = new void*[allocSize];
	size = 0;
}






template <class T> 
fddStorage<T>::fddStorage(size_t s){
	allocSize = s;
	localData = new T[s];
	size = s;
}
template <class T> 
fddStorage<T *>::fddStorage(size_t s){
	allocSize = s;
	localData = new T*[s];
	lineSizes = new size_t[s];
	size = s;
}		
fddStorage<std::string>::fddStorage(size_t s){
	allocSize = s;
	localData = new std::string[s];
	size = s;
}
fddStorage<void *>::fddStorage(size_t s){
	allocSize = s;
	localData = new void*[s];
	lineSizes = new size_t[s];
	size = s;
}





template <class T> 
fddStorage<T>::fddStorage(T * data, size_t s) : fddStorage(s){
	setData(data, s);
}

template <class T> 
fddStorage<T *>::fddStorage(T ** data, size_t * lineSizes, size_t s) : fddStorage(s){
	setData((void **) data, lineSizes, s);
}
fddStorage<std::string>::fddStorage(std::string * data, size_t s) : fddStorage(s){
	setData(data, s);
}
fddStorage<void *>::fddStorage(void ** data, size_t * lineSizes, size_t s) : fddStorage(s){
	setData(data, lineSizes, s);
}





template <class T> 
fddStorage<T>::~fddStorage(){
	if (localData != NULL){
		delete [] localData;
	}
}
template <class T> 
fddStorage<T *>::~fddStorage(){
	if (localData != NULL){
		delete [] localData;
		delete [] lineSizes;
	}
}		
fddStorage<std::string>::~fddStorage(){
	if (localData != NULL){
		delete [] localData;
	}
}
fddStorage<void *>::~fddStorage(){
	if (localData != NULL){
		delete [] localData;
		delete [] lineSizes;
	}
}






template <class T> 
void fddStorage<T>::setData( void * data, size_t s){
	grow(s / sizeof(T));
	memcpy(localData, data, s );
	size = s / sizeof(T);
}
template <class T> 
void fddStorage<T *>::setData( void * data, size_t s){
	std::cerr << "ERROR: Something went wrong in the code\n";
}
void fddStorage<std::string>::setData( void * data, size_t s){
	grow(s / sizeof(std::string));
	for ( size_t i = 0; i < s; ++i){
		localData[i] = ((std::string*) data)[s];
	}
	size = s / sizeof(std::string);
}
void fddStorage<void *>::setData( void * data, size_t s){
	std::cerr << "ERROR: Something went wrong in the code\n";
}





template <class T> 
void fddStorage<T>::setData( void ** data, size_t * lineSizes, size_t s){
	std::cerr << "ERROR: Something went wrong in the code\n";
}

template <class T> 
void fddStorage<T *>::setData( void ** data, size_t * lineSizes, size_t s){
	grow(s);
	for ( int i = 0; i < s; ++i){
		localData[i] = (T *) new  T [lineSizes[i]];
		memcpy(localData[i], data[i], lineSizes[i]*sizeof(T) );
	}
	size = s;
}
void fddStorage<std::string>::setData( void ** data, size_t * lineSizes, size_t s){
	std::cerr << "ERROR: Something went wrong in the code\n";
}
void fddStorage<void *>::setData( void ** data, size_t * lineSizes, size_t s){
	grow(s);
	for ( size_t i = 0; i < s; ++i){
		localData[i] = (void *) new  char [lineSizes[i]];
		memcpy(localData[i], data[i], lineSizes[i] );
	}
	size = s;
}





template <class T> 
T * fddStorage<T>::getData(){ 
	return localData; 
}
template <class T> 
T ** fddStorage<T *>::getData(){ 
	return localData; 
}
std::string * fddStorage<std::string>::getData(){ 
	return localData; 
}
void ** fddStorage<void *>::getData(){ 
	return localData; 
}




template <class T> 
size_t * fddStorage<T>::getLineSizes(){ 
	return NULL; 
}
template <class T> 
size_t * fddStorage<T *>::getLineSizes(){ 
	return lineSizes; 
}
size_t * fddStorage<void *>::getLineSizes(){ 
	return lineSizes; 
}






template <class T> 
T & fddStorage<T>::operator[](size_t ref){ 
	return localData[ref]; 
}
template <class T> 
T * & fddStorage<T *>::operator[](size_t ref){ 
	return localData[ref]; 
}
std::string & fddStorage<std::string>::operator[](size_t ref){ 
	return localData[ref]; 
}
void * & fddStorage<void *>::operator[](size_t ref){ 
	return localData[ref]; 
}




template <class T> 
void fddStorage<T>::grow(size_t toSize){
	if (allocSize < toSize){
		if ((allocSize * 2) > toSize){
			toSize = allocSize * 2;
		}

		T * newStorage = new T [toSize];

		if (size >0) 
			memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] localData;

		localData = newStorage;
		allocSize = toSize;
	}
}
template <class T> 
void fddStorage<T *>::grow(size_t toSize){
	if (allocSize < toSize){
		if ((allocSize * 2) > toSize){
			toSize = allocSize * 2;
		}

		size_t * newLineSizes = new size_t [toSize];
		T ** newStorage = new T* [toSize];

		if (size > 0){
			memcpy(newStorage, localData, size * sizeof( T* ) );
			memcpy(newLineSizes, lineSizes, size * sizeof( size_t ) );
		}

		delete [] localData;
		delete [] lineSizes;

		localData = newStorage;
		lineSizes = newLineSizes;
		allocSize = toSize;

	}
}
void fddStorage<std::string>::grow(size_t toSize){
	if (allocSize < toSize){
		if ((allocSize * 2) > toSize){
			toSize = allocSize * 2;
		}

		std::string * newStorage = new std::string [toSize];

		if (size >0) 
			for ( size_t i = 0; i < size; ++i)
				newStorage[i] = localData[i];

		delete [] localData;

		localData = newStorage;
		allocSize = toSize;
	}
}
void fddStorage<void *>::grow(size_t toSize){
	if (allocSize < toSize){
		if ((allocSize * 2) > toSize){
			toSize = allocSize * 2;
		}

		size_t * newLineSizes = new size_t [toSize];
		void ** newStorage = new void* [toSize];

		if (size > 0){
			memcpy(newStorage, localData, size * sizeof( void* ) );
			memcpy(newLineSizes, lineSizes, size * sizeof( size_t ) );
		}

		delete [] localData;
		delete [] lineSizes;

		localData = newStorage;
		lineSizes = newLineSizes;
		allocSize = toSize;

	}
}





template <class T> 
void fddStorage<T>::shrink(){
	if ( (size > 0) && (allocSize > size) ){
		T * newStorage = new T [size];

		memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] localData;

		localData = newStorage;
		allocSize = size;
	}
}
template <class T> 
void fddStorage<T *>::shrink(){
	if ( (size > 0) && (allocSize > size) ){
		T ** newStorage = new T* [size];

		memcpy(newStorage, localData, size * sizeof( T* ) );

		delete [] localData;

		localData = newStorage;
		allocSize = size;
	}
}
void fddStorage<std::string>::shrink(){
	if ( (size > 0) && (allocSize > size) ){
		std::string * newStorage = new std::string [size];

		for ( size_t i = 0; i < size; ++i)
			newStorage[i] = localData[i];

		delete [] localData;

		localData = newStorage;
		allocSize = size;
	}
}
void fddStorage<void *>::shrink(){
	if ( (size > 0) && (allocSize > size) ){
		void ** newStorage = new void* [size];

		memcpy(newStorage, localData, size * sizeof( void* ) );

		delete [] localData;

		localData = newStorage;
		allocSize = size;
	}
}






template <class T> 
void fddStorage<T>::insert(T & item){
	grow(size + 1);
	localData[size++] = item;	
}

template <class T> 
void fddStorage<T *>::insert(T *& item, size_t s){
	grow(size + 1);
	lineSizes[size] = s;
	localData[size++] = item;	
}

void fddStorage<std::string>::insert(std::string & item){
	grow(size + 1);
	localData[size++] = item;	
}
void fddStorage<void *>::insert(void *& item, size_t s){
	grow(size + 1);
	lineSizes[size] = s;
	localData[size++] = item;	
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
//template class fddStorage<std::vector<char>>;
//template class fddStorage<std::vector<int>>;
//template class fddStorage<std::vector<long int>>;
//template class fddStorage<std::vector<float>>;
//template class fddStorage<std::vector<double>>;
