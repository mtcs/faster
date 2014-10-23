#ifndef LIBFASTER_INDEXEDFDDSTORAGE_H
#define LIBFASTER_INDEXEDFDDSTORAGE_H


#include <cstdlib>

#include "definitions.h"
#include "fddStorageBase.h"

namespace faster {

	template <class K, class T> class indexedFddStorage;

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
			void setSize(size_t s UNUSED) {};

			T & operator[](size_t ref);

			void sortByKey();
	};

	// FDD storage place
	// Stores worker's FDD data locally
	template <class K, class T> 
	class indexedFddStorage : public indexedFddStorageCore<K, T> {
		public:
			indexedFddStorage();
			indexedFddStorage(size_t s);
			indexedFddStorage(K * keys, T * data, size_t s);
			void setData( K * keys, T * data, size_t s);
			void setDataRaw( void * keys, void * data, size_t s);

			void setSize(size_t s) override;

			void insert(K key, T & item);
			void insertRaw(void * d, size_t s);

			void grow(size_t toSize);
			void shrink();
	};

	template <class K, class T> 
	class indexedFddStorage <K, T *> : public indexedFddStorageCore<K, T *> {
		private:
			size_t * lineSizes;

		public:
			indexedFddStorage();
			indexedFddStorage(size_t s);
			indexedFddStorage(K * keys, T ** data, size_t * lineSizes, size_t s);

			~indexedFddStorage();

			void setData( K * keys, T ** data, size_t * lineSizes, size_t s);
			void setDataRaw( void * keys, void * data, size_t * lineSizes, size_t s);
			void setSize(size_t s) override;

			void insert(K key, T *& item, size_t s);
			void insertRaw(void * d, size_t s);

			size_t * getLineSizes();

			void grow(size_t toSize);
			void shrink();
	};


}

#endif
