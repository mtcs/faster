#include <string>

#include "indexedFddStorage.h"

template <class K, class T> 
indexedFddStorageCore<K,T>::indexedFddStorageCore(){
	allocSize = 200;
	localData = new T[allocSize];
	size = 0;
}
template <class K, class T> 
indexedFddStorage<K,T>::indexedFddStorage() : indexedFddStorageCore<K,T>(){}
template <class K, class T> 
indexedFddStorage<K,T*>::indexedFddStorage() : indexedFddStorageCore<K,T*>(){}

template <class K, class T> 
indexedFddStorageCore<K,T>::indexedFddStorageCore(size_t s){
	allocSize = s;
	localData = new T[s];
	size = s;
}




template <class K, class T> 
indexedFddStorage<K,T>::indexedFddStorage(K * keys, T * data, size_t s) : indexedFddStorageCore<K,T>(s){
	setData(keys, data, s);
}

template <class K, class T> 
indexedFddStorage<K,T*>::indexedFddStorage(K * keys, T ** data, size_t * lineSizes, size_t s) : indexedFddStorageCore<K,T *>(s){
	setData(keys, data, lineSizes, s);
}




template <class K, class T> 
indexedFddStorageCore<K,T>::~indexedFddStorageCore(){
	if (localData != NULL){
		delete [] localData;
	}
}
template <class K, class T> 
indexedFddStorage<K,T*>::~indexedFddStorage(){
	if (lineSizes != NULL){
		delete [] lineSizes;
	}
}		




template <class K, class T> 
void indexedFddStorage<K,T>::setData(K * keys, T * data, size_t s){
	grow(s / sizeof(T));
	//memcpy(localData, data, s );
	for ( size_t i = 0; i < s; ++i){
		this->localData[i] = ((T*) data)[i];
		this->localKeys[i] = keys[i];
	}
	this->size = s / sizeof(T);
}

template <class K, class T> 
void indexedFddStorage<K,T*>::setData( K * keys, T ** data, size_t * lineSizes, size_t s){
	grow(s);
	for ( int i = 0; i < s; ++i){
		this->localData[i] = (T *) new  T [lineSizes[i]];
		//memcpy(localData[i], data[i], lineSizes[i] );
		for ( int j = 0; j < lineSizes[i]; ++j){
			this->localData[i][j] =  ((T *) data[i])[j];
			this->localKeys[i] = keys[i];
		}
	}
	this->size = s;
}

template <class K, class T> 
void   indexedFddStorage<K,T>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}
template <class K, class T> 
void   indexedFddStorage<K,T*>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}


template <class K, class T> 
void indexedFddStorage<K,T>::insert(K key, T & item){
	grow(this->size + 1);
	this->localKeys[this->size] = key;
	this->localData[this->size++] = item;	
}

template <class K, class T> 
void indexedFddStorage<K,T*>::insert(K key, T *& item, size_t s){
	grow(this->size + 1);
	lineSizes[this->size] = s;	
	this->localKeys[this->size] = key;
	this->localData[this->size++] = item;	

}




template <class K, class T> 
T * indexedFddStorageCore<K,T>::getData(){ 
	return localData; 
}

template <class K, class T> 
K * indexedFddStorageCore<K,T>::getKeys(){ 
	return localKeys; 
}



template <class K, class T> 
size_t * indexedFddStorage<K,T*>::getLineSizes(){ 
	return lineSizes; 
}




template <class K, class T> 
T & indexedFddStorageCore<K,T>::operator[](size_t ref){ 
	return localData[ref]; 
}



template <class K, class T> 
void indexedFddStorage<K,T>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		T * newStorage = new T [toSize];
		K * newKeys = new K [toSize];

		if (this->size >0) 
			for ( size_t i = 0; i < this->size; ++i){
				newStorage[i] = this->localData[i];
				newKeys[i] = this->localKeys[i];
			}
			//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;
		delete [] this->localKeys;

		this->localData = newStorage;
		this->localKeys = newKeys;
		this->allocSize = toSize;
	}
}
template <class K, class T> 
void indexedFddStorage<K,T*>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		size_t * newLineSizes = new size_t [toSize];
		T ** newStorage = new T* [toSize];
		K * newKeys = new K [toSize];

		if (this->size > 0){
			//memcpy(newStorage, localData, this->size * sizeof( T* ) );
			//memcpy(newLineSizes, lineSizes, this->size * sizeof( size_t ) );
			for ( int i = 0; i < this->size; ++i){
				newStorage[i] =  this->localData[i];
				newLineSizes[i] = lineSizes[i];
				newKeys[i] = this->localKeys[i];
			}
		}

		delete [] this->localData;
		delete [] this->localKeys;
		delete [] lineSizes;

		this->localData = newStorage;
		this->localKeys = newKeys;
		lineSizes = newLineSizes;
		this->allocSize = toSize;

	}
}






template <class K, class T> 
void indexedFddStorage<K,T>::shrink(){
	if ( (this->size > 0) && (this->allocSize > this->size) ){
		T * newStorage = new T [this->size];
		K * newKeys = new K [this->size];

		for ( size_t i = 0; i < this->size; ++i){
			newStorage[i] = this->localData[i];
			newKeys[i] = this->localKeys[i];
		}
		//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;
		delete [] this->localKeys;

		this->localData = newStorage;
		this->localKeys = newKeys;
		this->allocSize = this->size;
	}
}
template <class K, class T> 
void indexedFddStorage<K,T*>::shrink(){
	if ( (this->size > 0) && (this->allocSize > this->size) ){
		T ** newStorage = new T* [this->size];
		K * newKeys = new K [this->size];
		size_t * newLineSizes = new size_t[this->size];

		for ( size_t i = 0; i < this->size; ++i){
			newStorage[i] = this->localData[i];
			newLineSizes[i] = lineSizes[i];
			newKeys[i] = this->localKeys[i];
		}
		//memcpy(newStorage, localData, size * sizeof( T ) );

		delete [] this->localData;
		delete [] this->localKeys;
		delete [] lineSizes;

		this->localData = newStorage;
		lineSizes = newLineSizes;
		this->localKeys = newKeys;
		this->allocSize = this->size;
	}
}

template class indexedFddStorageCore<char, char>;
template class indexedFddStorageCore<char, int>;
template class indexedFddStorageCore<char, long int>;
template class indexedFddStorageCore<char, float>;
template class indexedFddStorageCore<char, double>;
template class indexedFddStorageCore<char, char *>;
template class indexedFddStorageCore<char, int *>;
template class indexedFddStorageCore<char, long int *>;
template class indexedFddStorageCore<char, float *>;
template class indexedFddStorageCore<char, double *>;
template class indexedFddStorageCore<char, std::string>;
//template class indexedFddStorageCore<char, std::vector<char>>;
//template class indexedFddStorageCore<char, std::vector<int>>;
//template class indexedFddStorageCore<char, std::vector<long int>>;
//template class indexedFddStorageCore<char, std::vector<float>>;
//template class indexedFddStorageCore<char, std::vector<double>>;

template class indexedFddStorageCore<int, char>;
template class indexedFddStorageCore<int, int>;
template class indexedFddStorageCore<int, long int>;
template class indexedFddStorageCore<int, float>;
template class indexedFddStorageCore<int, double>;
template class indexedFddStorageCore<int, char *>;
template class indexedFddStorageCore<int, int *>;
template class indexedFddStorageCore<int, long int *>;
template class indexedFddStorageCore<int, float *>;
template class indexedFddStorageCore<int, double *>;
template class indexedFddStorageCore<int, std::string>;
//template class indexedFddStorageCore<int, std::vector<char>>;
//template class indexedFddStorageCore<int, std::vector<int>>;
//template class indexedFddStorageCore<int, std::vector<long int>>;
//template class indexedFddStorageCore<int, std::vector<float>>;
//template class indexedFddStorageCore<int, std::vector<double>>;

template class indexedFddStorageCore<long int, char>;
template class indexedFddStorageCore<long int, int>;
template class indexedFddStorageCore<long int, long int>;
template class indexedFddStorageCore<long int, float>;
template class indexedFddStorageCore<long int, double>;
template class indexedFddStorageCore<long int, char *>;
template class indexedFddStorageCore<long int, int *>;
template class indexedFddStorageCore<long int, long int *>;
template class indexedFddStorageCore<long int, float *>;
template class indexedFddStorageCore<long int, double *>;
template class indexedFddStorageCore<long int, std::string>;
//template class indexedFddStorageCore<long int, std::vector<char>>;
//template class indexedFddStorageCore<long int, std::vector<int>>;
//template class indexedFddStorageCore<long int, std::vector<long int>>;
//template class indexedFddStorageCore<long int, std::vector<float>>;
//template class indexedFddStorageCore<long int, std::vector<double>>;

template class indexedFddStorageCore<float, char>;
template class indexedFddStorageCore<float, int>;
template class indexedFddStorageCore<float, long int>;
template class indexedFddStorageCore<float, float>;
template class indexedFddStorageCore<float, double>;
template class indexedFddStorageCore<float, char *>;
template class indexedFddStorageCore<float, int *>;
template class indexedFddStorageCore<float, long int *>;
template class indexedFddStorageCore<float, float *>;
template class indexedFddStorageCore<float, double *>;
template class indexedFddStorageCore<float, std::string>;
//template class indexedFddStorageCore<float, std::vector<char>>;
//template class indexedFddStorageCore<float, std::vector<int>>;
//template class indexedFddStorageCore<float, std::vector<long int>>;
//template class indexedFddStorageCore<float, std::vector<float>>;
//template class indexedFddStorageCore<float, std::vector<double>>;

template class indexedFddStorageCore<double, char>;
template class indexedFddStorageCore<double, int>;
template class indexedFddStorageCore<double, long int>;
template class indexedFddStorageCore<double, float>;
template class indexedFddStorageCore<double, double>;
template class indexedFddStorageCore<double, char *>;
template class indexedFddStorageCore<double, int *>;
template class indexedFddStorageCore<double, long int *>;
template class indexedFddStorageCore<double, float *>;
template class indexedFddStorageCore<double, double *>;
template class indexedFddStorageCore<double, std::string>;
//template class indexedFddStorageCore<double, std::vector<char>>;
//template class indexedFddStorageCore<double, std::vector<int>>;
//template class indexedFddStorageCore<double, std::vector<long int>>;
//template class indexedFddStorageCore<double, std::vector<float>>;
//template class indexedFddStorageCore<double, std::vector<double>>;

template class indexedFddStorageCore<std::string, char>;
template class indexedFddStorageCore<std::string, int>;
template class indexedFddStorageCore<std::string, long int>;
template class indexedFddStorageCore<std::string, float>;
template class indexedFddStorageCore<std::string, double>;
template class indexedFddStorageCore<std::string, char *>;
template class indexedFddStorageCore<std::string, int *>;
template class indexedFddStorageCore<std::string, long int *>;
template class indexedFddStorageCore<std::string, float *>;
template class indexedFddStorageCore<std::string, double *>;
template class indexedFddStorageCore<std::string, std::string>;
//template class indexedFddStorageCore<std::string, std::vector<char>>;
//template class indexedFddStorageCore<std::string, std::vector<int>>;
//template class indexedFddStorageCore<std::string, std::vector<long int>>;
//template class indexedFddStorageCore<std::string, std::vector<float>>;
//template class indexedFddStorageCore<std::string, std::vector<double>>;


template class indexedFddStorage<char, char>;
template class indexedFddStorage<char, int>;
template class indexedFddStorage<char, long int>;
template class indexedFddStorage<char, float>;
template class indexedFddStorage<char, double>;
template class indexedFddStorage<char, char *>;
template class indexedFddStorage<char, int *>;
template class indexedFddStorage<char, long int *>;
template class indexedFddStorage<char, float *>;
template class indexedFddStorage<char, double *>;
template class indexedFddStorage<char, std::string>;
//template class indexedFddStorage<char, std::vector<char>>;
//template class indexedFddStorage<char, std::vector<int>>;
//template class indexedFddStorage<char, std::vector<long int>>;
//template class indexedFddStorage<char, std::vector<float>>;
//template class indexedFddStorage<char, std::vector<double>>;

template class indexedFddStorage<int, char>;
template class indexedFddStorage<int, int>;
template class indexedFddStorage<int, long int>;
template class indexedFddStorage<int, float>;
template class indexedFddStorage<int, double>;
template class indexedFddStorage<int, char *>;
template class indexedFddStorage<int, int *>;
template class indexedFddStorage<int, long int *>;
template class indexedFddStorage<int, float *>;
template class indexedFddStorage<int, double *>;
template class indexedFddStorage<int, std::string>;
//template class indexedFddStorage<int, std::vector<char>>;
//template class indexedFddStorage<int, std::vector<int>>;
//template class indexedFddStorage<int, std::vector<long int>>;
//template class indexedFddStorage<int, std::vector<float>>;
//template class indexedFddStorage<int, std::vector<double>>;

template class indexedFddStorage<long int, char>;
template class indexedFddStorage<long int, int>;
template class indexedFddStorage<long int, long int>;
template class indexedFddStorage<long int, float>;
template class indexedFddStorage<long int, double>;
template class indexedFddStorage<long int, char *>;
template class indexedFddStorage<long int, int *>;
template class indexedFddStorage<long int, long int *>;
template class indexedFddStorage<long int, float *>;
template class indexedFddStorage<long int, double *>;
template class indexedFddStorage<long int, std::string>;
//template class indexedFddStorage<long int, std::vector<char>>;
//template class indexedFddStorage<long int, std::vector<int>>;
//template class indexedFddStorage<long int, std::vector<long int>>;
//template class indexedFddStorage<long int, std::vector<float>>;
//template class indexedFddStorage<long int, std::vector<double>>;

template class indexedFddStorage<float, char>;
template class indexedFddStorage<float, int>;
template class indexedFddStorage<float, long int>;
template class indexedFddStorage<float, float>;
template class indexedFddStorage<float, double>;
template class indexedFddStorage<float, char *>;
template class indexedFddStorage<float, int *>;
template class indexedFddStorage<float, long int *>;
template class indexedFddStorage<float, float *>;
template class indexedFddStorage<float, double *>;
template class indexedFddStorage<float, std::string>;
//template class indexedFddStorage<float, std::vector<char>>;
//template class indexedFddStorage<float, std::vector<int>>;
//template class indexedFddStorage<float, std::vector<long int>>;
//template class indexedFddStorage<float, std::vector<float>>;
//template class indexedFddStorage<float, std::vector<double>>;

template class indexedFddStorage<double, char>;
template class indexedFddStorage<double, int>;
template class indexedFddStorage<double, long int>;
template class indexedFddStorage<double, float>;
template class indexedFddStorage<double, double>;
template class indexedFddStorage<double, char *>;
template class indexedFddStorage<double, int *>;
template class indexedFddStorage<double, long int *>;
template class indexedFddStorage<double, float *>;
template class indexedFddStorage<double, double *>;
template class indexedFddStorage<double, std::string>;
//template class indexedFddStorage<double, std::vector<char>>;
//template class indexedFddStorage<double, std::vector<int>>;
//template class indexedFddStorage<double, std::vector<long int>>;
//template class indexedFddStorage<double, std::vector<float>>;
//template class indexedFddStorage<double, std::vector<double>>;

template class indexedFddStorage<std::string, char>;
template class indexedFddStorage<std::string, int>;
template class indexedFddStorage<std::string, long int>;
template class indexedFddStorage<std::string, float>;
template class indexedFddStorage<std::string, double>;
template class indexedFddStorage<std::string, char *>;
template class indexedFddStorage<std::string, int *>;
template class indexedFddStorage<std::string, long int *>;
template class indexedFddStorage<std::string, float *>;
template class indexedFddStorage<std::string, double *>;
template class indexedFddStorage<std::string, std::string>;
//template class indexedFddStorage<std::string, std::vector<char>>;
//template class indexedFddStorage<std::string, std::vector<int>>;
//template class indexedFddStorage<std::string, std::vector<long int>>;
//template class indexedFddStorage<std::string, std::vector<float>>;
//template class indexedFddStorage<std::string, std::vector<double>>;


