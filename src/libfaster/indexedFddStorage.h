#ifndef LIBFASTER_FDDSTORAGE_H
#define LIBFASTER_FDDSTORAGE_H

template <class T> class fddStorage;

#include "misc.h"

class fddStorageBase{
	protected:
		size_t size;
		size_t allocSize;

	public:
		virtual void grow(size_t toSize){ }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }
};


// FDD storage place
// Stores worker's FDD data locally
template <class T> 
class fddStorage : public fddStorageBase {
	private:
		T * localData;
	public:
		fddStorage();
		fddStorage(size_t s);
		fddStorage(T * data, size_t s);

		~fddStorage();

		void setData( void * data, size_t s);
		void setData( void ** data, size_t * lineSizes, size_t s);

		T * getData();
		size_t * getLineSizes();

		 T & operator[](size_t ref);

		 void grow(size_t toSize);
		 void shrink();
		 void insert(T & item);

};

template <class T> 
class fddStorage <T *> : public fddStorageBase {
	private:
		size_t * lineSizes;
		T ** localData;

	public:
		fddStorage();
		fddStorage(size_t s);
		fddStorage(T ** data, size_t * lineSizes, size_t s);

		~fddStorage();

		void setData( void * data, size_t s);
		void setData( void ** data, size_t * lineSizes, size_t s);

		T ** getData();
		size_t * getLineSizes();

		 T * & operator[](size_t ref);

		 void grow(size_t toSize);
		 void shrink();
		 void insert(T *& item, size_t s);
};

template <> 
class fddStorage <std::string> : public fddStorageBase {
	private:
		std::string * localData;
	public:
		fddStorage();
		fddStorage(size_t s);
		fddStorage(std::string * data, size_t s);
		~fddStorage();

		void setData( void * data, size_t s);
		void setData( void ** data, size_t * lineSizes, size_t s);

		std::string * getData();
		std::string & operator[](size_t ref);

		 void grow(size_t toSize);
		 void shrink();
	
		 void insert(std::string & item);
	
};

template <> 
class fddStorage <void *> : public fddStorageBase {
	private:
		size_t * lineSizes;

		void ** localData;
	public:
		fddStorage();
		fddStorage(size_t s);
		fddStorage(void ** data, size_t * lineSizes, size_t s);
		~fddStorage();

		void setData( void * data, size_t s);
		void setData( void ** data, size_t * lineSizes, size_t s);

		void ** getData();
		size_t * getLineSizes();

		 void * & operator[](size_t ref);

		 void grow(size_t toSize);
		 void shrink();

		 void insert(void *& item, size_t s);

};



#endif
