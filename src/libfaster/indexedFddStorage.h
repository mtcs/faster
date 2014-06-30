#ifndef LIBFASTER_INDEXEDFDDSTORAGE_H
#define LIBFASTER_INDEXEDFDDSTORAGE_H

template <class K, class T> class indexedFddStorage;

#include <cstdlib>
#include "fddStorageBase.h"

template <class K, class T> 
class indexedFddStorageCore : public fddStorageBase {
	protected:
		T * localData;
		K * localKeys;
	public:
		indexedFddStorageCore();
		indexedFddStorageCore(size_t s);

		~indexedFddStorageCore();

		T * getData();
		K * getKeys();

		T & operator[](size_t ref);


};

// FDD storage place
// Stores worker's FDD data locally
template <class K, class T> 
class indexedFddStorage : public indexedFddStorageCore<K, T> {
	public:
		indexedFddStorage();
		indexedFddStorage(K * keys, T * data, size_t s);
		void setData( K * keys, T * data, size_t s);

		void insert(K key, T & item);

		void grow(size_t toSize);
		void shrink();
};

template <class K, class T> 
class indexedFddStorage <K, T *> : public indexedFddStorageCore<K, T *> {
	private:
		size_t * lineSizes;

	public:
		indexedFddStorage();
		indexedFddStorage(K * keys, T ** data, size_t * lineSizes, size_t s);

		~indexedFddStorage();

		void setData( K * keys, T ** data, size_t * lineSizes, size_t s);

		void insert(K key, T *& item, size_t s);

		size_t * getLineSizes();

		void grow(size_t toSize);
		void shrink();
};




#endif
