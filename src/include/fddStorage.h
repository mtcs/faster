#ifndef LIBFASTER_FDDSTORAGE_H
#define LIBFASTER_FDDSTORAGE_H


#include <cstdlib>
#include "fddStorageBase.h"

namespace faster{

	template <class T> class fddStorage;

	template <class T> 
	class fddStorageCore : public fddStorageBase {
		protected:
			T * localData;
		public:
			fddStorageCore();
			fddStorageCore(size_t s);

			~fddStorageCore();

			T * getData();
			void   setSize(size_t s UNUSED) {};

			T & operator[](size_t ref);


	};

	// FDD storage place
	// Stores worker's FDD data locally
	template <class T> 
	class fddStorage : public fddStorageCore<T> {
		public:
			fddStorage();
			fddStorage(size_t s):fddStorageCore<T>(s){}
			fddStorage(T * data, size_t s);
			void setData( T * data, size_t s);
			void setDataRaw( void * data, size_t s);
			
			void setSize(size_t s) override;

			void insert(T & item);

			void grow(size_t toSize);
			void shrink();
	};

	template <class T> 
	class fddStorage <T *> : public fddStorageCore<T *> {
		private:
			size_t * lineSizes;

		public:
			fddStorage();
			fddStorage(size_t s);
			fddStorage(T ** data, size_t * lineSizes, size_t s);

			~fddStorage();

			void setData( T ** data, size_t * lineSizes, size_t s);
			void setDataRaw( void * data, size_t * lineSizes, size_t s);
			void setSize(size_t s) override;

			void insert(T *& item, size_t s);

			size_t * getLineSizes();

			void grow(size_t toSize);
			void shrink();
	};

}

#endif
