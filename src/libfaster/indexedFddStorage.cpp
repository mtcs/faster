#include <string>
#include <algorithm>

#include "fastCommBuffer.h"
#include "indexedFddStorage.h"

template <class K, class T> 
faster::indexedFddStorageCore<K,T>::indexedFddStorageCore(){
	allocSize = 1000;
	localData = new T[allocSize];
	localKeys = new K[allocSize];
	size = 0;
}

template <class K, class T> 
faster::indexedFddStorageCore<K,T>::indexedFddStorageCore(size_t s){
	if (s > 0){
		allocSize = s;
	}else{
		allocSize = 1000;
	}
	localData = new T[allocSize];
	localKeys = new K[allocSize];
	size = s;
}
template <class K, class T> 
faster::indexedFddStorageCore<K,T>::~indexedFddStorageCore(){
	if (localData != NULL){
		delete [] localData;
		delete [] localKeys;
	}
}

template <class K, class T> 
T * faster::indexedFddStorageCore<K,T>::getData(){ 
	return localData; 
}

template <class K, class T> 
K * faster::indexedFddStorageCore<K,T>::getKeys(){ 
	return localKeys; 
}
template <class K, class T> 
T & faster::indexedFddStorageCore<K,T>::operator[](size_t ref){ 
	return localData[ref]; 
}

template <class K, class T> 
void faster::indexedFddStorageCore<K,T>::sortByKey(){
	//std::cerr << "        SortByKey";
	std::vector<size_t> p(size,0);
	std::vector<size_t> rp(size);
	std::vector<bool> sorted(size, false);
	size_t i = 0;

	// Sort
	std::iota(p.begin(), p.end(), 0);
	std::sort(p.begin(), p.end(),
			        [&](size_t i, size_t j){ return localKeys[i] < localKeys[j]; });
	
	// Apply in-place
	
	// get reverse permutation item>position
	for (i = 0; i < size; ++i){
		rp[p[i]] = i;
	}

	i = 0;
	K savedKey;
	T savedData;
	while ( i < size){
		size_t pos = i;
		// Save This element;
		if ( ! sorted[pos] ){
			savedKey = localKeys[p[pos]];
			savedData = localData[p[pos]];
		}
		while ( ! sorted[pos] ){
			// Hold item to be replaced
			K holdenKey  = localKeys[pos];
			T holdenData = localData[pos];
			// Save where it should go
			size_t holdenPos = rp[pos];

			// Replace 
			localKeys[pos] = savedKey;
			localData[pos] = savedData;

			// Get last item to be the pivot
			savedKey = holdenKey;
			savedData = holdenData;

			// Mark this item as sorted
			sorted[pos] = true;

			// Go to the saved item proper location
			pos = holdenPos;
		}
		++i;
	}
}




template <class K, class T> 
faster::indexedFddStorage<K,T>::indexedFddStorage() : indexedFddStorageCore<K,T>(){}
template <class K, class T> 
faster::indexedFddStorage<K,T*>::indexedFddStorage() : indexedFddStorageCore<K,T*>(){}

template <class K, class T> 
faster::indexedFddStorage<K,T>::indexedFddStorage(size_t s) : indexedFddStorageCore<K,T>(s){
}

template <class K, class T> 
faster::indexedFddStorage<K,T*>::indexedFddStorage(size_t s) : indexedFddStorageCore<K,T*>(s){
	lineSizes = new size_t[s];
}


template <class K, class T> 
faster::indexedFddStorage<K,T>::indexedFddStorage(K * keys, T * data, size_t s) : indexedFddStorage<K,T>(s){
	setData(keys, data, s);
}

template <class K, class T> 
faster::indexedFddStorage<K,T*>::indexedFddStorage(K * keys, T ** data, size_t * lineSizes, size_t s) : indexedFddStorage<K,T *>(s){
	setData(keys, data, lineSizes, s);
}




template <class K, class T> 
faster::indexedFddStorage<K,T*>::~indexedFddStorage(){
	if (lineSizes != NULL){
		delete [] lineSizes;
	}
}		




template <class K, class T> 
void faster::indexedFddStorage<K,T>::setData(K * keys, T * data, size_t s){
	grow(s);
	this->size = s;

	for ( size_t i = 0; i < s; ++i){
		this->localData[i] = data[i];
		this->localKeys[i] = keys[i];
	}
}

template <class K, class T> 
void faster::indexedFddStorage<K,T*>::setData( K * keys, T ** data, size_t * ls, size_t s){
	grow(s);
	#pragma omp parallel for
	for ( size_t i = 0; i < s; ++i){
		lineSizes[i] = ls[i];

		this->localData[i] = new  T [lineSizes[i]];
		for ( size_t j = 0; j < lineSizes[i]; ++j){
			this->localData[i][j] =  ((T *) data[i])[j];
		}
		this->localKeys[i] = keys[i];
	}
	this->size = s;
}
template <class K, class T> 
void faster::indexedFddStorage<K,T>::setDataRaw(void * keys, void * data, size_t s){
	fastCommBuffer buffer(0);
	fastCommBuffer buffer2(0);

	//grow(s);
	//this->size = s;
	buffer.setBuffer(data, s);
	buffer2.setBuffer(keys, s);

	//std::cerr << "\nindexedFddStorage setData ";
	for ( size_t i = 0; i < s; ++i){
		buffer >> this->localData[i];
		buffer2 >> this->localKeys[i];
	}
}
template <class K, class T> 
void faster::indexedFddStorage<K,T*>::setDataRaw( void * keys, void * data, size_t * ls, size_t s){
	fastCommBuffer buffer(0);
	fastCommBuffer buffer2(0);
	
	buffer.setBuffer(data, s);
	buffer2.setBuffer(keys, s);

	for ( size_t i = 0; i < s; ++i){
		lineSizes[i] = ls[i];

		this->localData[i] = new  T [lineSizes[i]];

		buffer2 >> this->localKeys[i];
		buffer.read(this->localData[i], ls[i]*sizeof(T));
	}
}

template <class K, class T> 
void   faster::indexedFddStorage<K,T>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}
template <class K, class T> 
void   faster::indexedFddStorage<K,T*>::setSize(size_t s){ 
	this->grow(s); 
	this->size = s;  
}


template <class K, class T> 
void faster::indexedFddStorage<K,T>::insert(K key, T & item){
	grow(this->size + 1);
	this->localKeys[this->size] = key;
	this->localData[this->size++] = item;	
}

template <class K, class T> 
void faster::indexedFddStorage<K,T*>::insert(K key, T *& item, size_t s){
	grow(this->size + 1);
	lineSizes[this->size] = s;	
	this->localKeys[this->size] = key;
	this->localData[this->size++] = item;	

}
			






template <class K, class T> 
size_t * faster::indexedFddStorage<K,T*>::getLineSizes(){ 
	return lineSizes; 
}




template <class K, class T> 
void faster::indexedFddStorage<K,T>::grow(size_t toSize){
	if (this->allocSize < toSize){
		if ((this->allocSize * 2) > toSize){
			toSize = this->allocSize * 2;
		}

		T * newStorage = new T [toSize];
		K * newKeys = new K [toSize];

		if (this->size >0) {
			#pragma omp parallel for
			for ( size_t i = 0; i < this->size; ++i){
				newStorage[i] = this->localData[i];
				newKeys[i] = this->localKeys[i];
			}
			//memcpy(newStorage, localData, size * sizeof( T ) );
		}

		delete [] this->localData;
		delete [] this->localKeys;

		this->localData = newStorage;
		this->localKeys = newKeys;
		this->allocSize = toSize;
	}
}
template <class K, class T> 
void faster::indexedFddStorage<K,T*>::grow(size_t toSize){
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
			#pragma omp parallel for
			for ( size_t i = 0; i < this->size; ++i){
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
void faster::indexedFddStorage<K,T>::shrink(){
	if ( (this->size > 0) && (this->allocSize > this->size) ){
		T * newStorage = new T [this->size];
		K * newKeys = new K [this->size];

		for ( size_t i = 0; i < this->size; ++i){
			newStorage[i] = this->localData[i];
			newKeys[i] = this->localKeys[i];
		}

		delete [] this->localData;
		delete [] this->localKeys;

		this->localData = newStorage;
		this->localKeys = newKeys;
		this->allocSize = this->size;
	}
}
template <class K, class T> 
void faster::indexedFddStorage<K,T*>::shrink(){
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

template class faster::indexedFddStorageCore<char, char>;
template class faster::indexedFddStorageCore<char, int>;
template class faster::indexedFddStorageCore<char, long int>;
template class faster::indexedFddStorageCore<char, float>;
template class faster::indexedFddStorageCore<char, double>;
template class faster::indexedFddStorageCore<char, char *>;
template class faster::indexedFddStorageCore<char, int *>;
template class faster::indexedFddStorageCore<char, long int *>;
template class faster::indexedFddStorageCore<char, float *>;
template class faster::indexedFddStorageCore<char, double *>;
template class faster::indexedFddStorageCore<char, std::string>;
template class faster::indexedFddStorageCore<char, std::vector<char>>;
template class faster::indexedFddStorageCore<char, std::vector<int>>;
template class faster::indexedFddStorageCore<char, std::vector<long int>>;
template class faster::indexedFddStorageCore<char, std::vector<float>>;
template class faster::indexedFddStorageCore<char, std::vector<double>>;

template class faster::indexedFddStorageCore<int, char>;
template class faster::indexedFddStorageCore<int, int>;
template class faster::indexedFddStorageCore<int, long int>;
template class faster::indexedFddStorageCore<int, float>;
template class faster::indexedFddStorageCore<int, double>;
template class faster::indexedFddStorageCore<int, char *>;
template class faster::indexedFddStorageCore<int, int *>;
template class faster::indexedFddStorageCore<int, long int *>;
template class faster::indexedFddStorageCore<int, float *>;
template class faster::indexedFddStorageCore<int, double *>;
template class faster::indexedFddStorageCore<int, std::string>;
template class faster::indexedFddStorageCore<int, std::vector<char>>;
template class faster::indexedFddStorageCore<int, std::vector<int>>;
template class faster::indexedFddStorageCore<int, std::vector<long int>>;
template class faster::indexedFddStorageCore<int, std::vector<float>>;
template class faster::indexedFddStorageCore<int, std::vector<double>>;

template class faster::indexedFddStorageCore<long int, char>;
template class faster::indexedFddStorageCore<long int, int>;
template class faster::indexedFddStorageCore<long int, long int>;
template class faster::indexedFddStorageCore<long int, float>;
template class faster::indexedFddStorageCore<long int, double>;
template class faster::indexedFddStorageCore<long int, char *>;
template class faster::indexedFddStorageCore<long int, int *>;
template class faster::indexedFddStorageCore<long int, long int *>;
template class faster::indexedFddStorageCore<long int, float *>;
template class faster::indexedFddStorageCore<long int, double *>;
template class faster::indexedFddStorageCore<long int, std::string>;
template class faster::indexedFddStorageCore<long int, std::vector<char>>;
template class faster::indexedFddStorageCore<long int, std::vector<int>>;
template class faster::indexedFddStorageCore<long int, std::vector<long int>>;
template class faster::indexedFddStorageCore<long int, std::vector<float>>;
template class faster::indexedFddStorageCore<long int, std::vector<double>>;

template class faster::indexedFddStorageCore<float, char>;
template class faster::indexedFddStorageCore<float, int>;
template class faster::indexedFddStorageCore<float, long int>;
template class faster::indexedFddStorageCore<float, float>;
template class faster::indexedFddStorageCore<float, double>;
template class faster::indexedFddStorageCore<float, char *>;
template class faster::indexedFddStorageCore<float, int *>;
template class faster::indexedFddStorageCore<float, long int *>;
template class faster::indexedFddStorageCore<float, float *>;
template class faster::indexedFddStorageCore<float, double *>;
template class faster::indexedFddStorageCore<float, std::string>;
template class faster::indexedFddStorageCore<float, std::vector<char>>;
template class faster::indexedFddStorageCore<float, std::vector<int>>;
template class faster::indexedFddStorageCore<float, std::vector<long int>>;
template class faster::indexedFddStorageCore<float, std::vector<float>>;
template class faster::indexedFddStorageCore<float, std::vector<double>>;

template class faster::indexedFddStorageCore<double, char>;
template class faster::indexedFddStorageCore<double, int>;
template class faster::indexedFddStorageCore<double, long int>;
template class faster::indexedFddStorageCore<double, float>;
template class faster::indexedFddStorageCore<double, double>;
template class faster::indexedFddStorageCore<double, char *>;
template class faster::indexedFddStorageCore<double, int *>;
template class faster::indexedFddStorageCore<double, long int *>;
template class faster::indexedFddStorageCore<double, float *>;
template class faster::indexedFddStorageCore<double, double *>;
template class faster::indexedFddStorageCore<double, std::string>;
template class faster::indexedFddStorageCore<double, std::vector<char>>;
template class faster::indexedFddStorageCore<double, std::vector<int>>;
template class faster::indexedFddStorageCore<double, std::vector<long int>>;
template class faster::indexedFddStorageCore<double, std::vector<float>>;
template class faster::indexedFddStorageCore<double, std::vector<double>>;

template class faster::indexedFddStorageCore<std::string, char>;
template class faster::indexedFddStorageCore<std::string, int>;
template class faster::indexedFddStorageCore<std::string, long int>;
template class faster::indexedFddStorageCore<std::string, float>;
template class faster::indexedFddStorageCore<std::string, double>;
template class faster::indexedFddStorageCore<std::string, char *>;
template class faster::indexedFddStorageCore<std::string, int *>;
template class faster::indexedFddStorageCore<std::string, long int *>;
template class faster::indexedFddStorageCore<std::string, float *>;
template class faster::indexedFddStorageCore<std::string, double *>;
template class faster::indexedFddStorageCore<std::string, std::string>;
template class faster::indexedFddStorageCore<std::string, std::vector<char>>;
template class faster::indexedFddStorageCore<std::string, std::vector<int>>;
template class faster::indexedFddStorageCore<std::string, std::vector<long int>>;
template class faster::indexedFddStorageCore<std::string, std::vector<float>>;
template class faster::indexedFddStorageCore<std::string, std::vector<double>>;


template class faster::indexedFddStorage<char, char>;
template class faster::indexedFddStorage<char, int>;
template class faster::indexedFddStorage<char, long int>;
template class faster::indexedFddStorage<char, float>;
template class faster::indexedFddStorage<char, double>;
template class faster::indexedFddStorage<char, char *>;
template class faster::indexedFddStorage<char, int *>;
template class faster::indexedFddStorage<char, long int *>;
template class faster::indexedFddStorage<char, float *>;
template class faster::indexedFddStorage<char, double *>;
template class faster::indexedFddStorage<char, std::string>;
template class faster::indexedFddStorage<char, std::vector<char>>;
template class faster::indexedFddStorage<char, std::vector<int>>;
template class faster::indexedFddStorage<char, std::vector<long int>>;
template class faster::indexedFddStorage<char, std::vector<float>>;
template class faster::indexedFddStorage<char, std::vector<double>>;

template class faster::indexedFddStorage<int, char>;
template class faster::indexedFddStorage<int, int>;
template class faster::indexedFddStorage<int, long int>;
template class faster::indexedFddStorage<int, float>;
template class faster::indexedFddStorage<int, double>;
template class faster::indexedFddStorage<int, char *>;
template class faster::indexedFddStorage<int, int *>;
template class faster::indexedFddStorage<int, long int *>;
template class faster::indexedFddStorage<int, float *>;
template class faster::indexedFddStorage<int, double *>;
template class faster::indexedFddStorage<int, std::string>;
template class faster::indexedFddStorage<int, std::vector<char>>;
template class faster::indexedFddStorage<int, std::vector<int>>;
template class faster::indexedFddStorage<int, std::vector<long int>>;
template class faster::indexedFddStorage<int, std::vector<float>>;
template class faster::indexedFddStorage<int, std::vector<double>>;

template class faster::indexedFddStorage<long int, char>;
template class faster::indexedFddStorage<long int, int>;
template class faster::indexedFddStorage<long int, long int>;
template class faster::indexedFddStorage<long int, float>;
template class faster::indexedFddStorage<long int, double>;
template class faster::indexedFddStorage<long int, char *>;
template class faster::indexedFddStorage<long int, int *>;
template class faster::indexedFddStorage<long int, long int *>;
template class faster::indexedFddStorage<long int, float *>;
template class faster::indexedFddStorage<long int, double *>;
template class faster::indexedFddStorage<long int, std::string>;
template class faster::indexedFddStorage<long int, std::vector<char>>;
template class faster::indexedFddStorage<long int, std::vector<int>>;
template class faster::indexedFddStorage<long int, std::vector<long int>>;
template class faster::indexedFddStorage<long int, std::vector<float>>;
template class faster::indexedFddStorage<long int, std::vector<double>>;

template class faster::indexedFddStorage<float, char>;
template class faster::indexedFddStorage<float, int>;
template class faster::indexedFddStorage<float, long int>;
template class faster::indexedFddStorage<float, float>;
template class faster::indexedFddStorage<float, double>;
template class faster::indexedFddStorage<float, char *>;
template class faster::indexedFddStorage<float, int *>;
template class faster::indexedFddStorage<float, long int *>;
template class faster::indexedFddStorage<float, float *>;
template class faster::indexedFddStorage<float, double *>;
template class faster::indexedFddStorage<float, std::string>;
template class faster::indexedFddStorage<float, std::vector<char>>;
template class faster::indexedFddStorage<float, std::vector<int>>;
template class faster::indexedFddStorage<float, std::vector<long int>>;
template class faster::indexedFddStorage<float, std::vector<float>>;
template class faster::indexedFddStorage<float, std::vector<double>>;

template class faster::indexedFddStorage<double, char>;
template class faster::indexedFddStorage<double, int>;
template class faster::indexedFddStorage<double, long int>;
template class faster::indexedFddStorage<double, float>;
template class faster::indexedFddStorage<double, double>;
template class faster::indexedFddStorage<double, char *>;
template class faster::indexedFddStorage<double, int *>;
template class faster::indexedFddStorage<double, long int *>;
template class faster::indexedFddStorage<double, float *>;
template class faster::indexedFddStorage<double, double *>;
template class faster::indexedFddStorage<double, std::string>;
template class faster::indexedFddStorage<double, std::vector<char>>;
template class faster::indexedFddStorage<double, std::vector<int>>;
template class faster::indexedFddStorage<double, std::vector<long int>>;
template class faster::indexedFddStorage<double, std::vector<float>>;
template class faster::indexedFddStorage<double, std::vector<double>>;

template class faster::indexedFddStorage<std::string, char>;
template class faster::indexedFddStorage<std::string, int>;
template class faster::indexedFddStorage<std::string, long int>;
template class faster::indexedFddStorage<std::string, float>;
template class faster::indexedFddStorage<std::string, double>;
template class faster::indexedFddStorage<std::string, char *>;
template class faster::indexedFddStorage<std::string, int *>;
template class faster::indexedFddStorage<std::string, long int *>;
template class faster::indexedFddStorage<std::string, float *>;
template class faster::indexedFddStorage<std::string, double *>;
template class faster::indexedFddStorage<std::string, std::string>;
template class faster::indexedFddStorage<std::string, std::vector<char>>;
template class faster::indexedFddStorage<std::string, std::vector<int>>;
template class faster::indexedFddStorage<std::string, std::vector<long int>>;
template class faster::indexedFddStorage<std::string, std::vector<float>>;
template class faster::indexedFddStorage<std::string, std::vector<double>>;


