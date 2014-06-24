#ifndef LIBFASTER_FDDSTORAGE_H
#define LIBFASTER_FDDSTORAGE_H

template <class T> class fddStorage;

#include "misc.h"


// FDD storage place
// Stores worker's FDD data locally
template <class T> 
class fddStorage {
	private:
		size_t size;
		size_t allocSize;

		T * localData;
	public:
		fddStorage(){
			allocSize = 200;
			localData = new T[allocSize];
			size = 0;
		}
		fddStorage(size_t s){
			allocSize = s;
			localData = new T[s];
			size = s;
		}
		fddStorage(T * data, size_t s) : fddStorage(s){
			memcpy(localData, data, s*sizeof(T) );
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( void * data, size_t s){
			grow(s / sizeof(T));
			memcpy(localData, data, s );
			size = s / sizeof(T);
		}
		void setData( void ** data, size_t * lineSizes, size_t s){
			std::cerr << "ERROR: Something went wrong in the code\n";
		}

		T * getData(){ return localData; }
		size_t * getLineSizes(){ return NULL; }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }

		 T & operator[](size_t ref){ return localData[ref]; }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) > toSize){
					toSize = allocSize * 1.8;
				}
				
				T * newStorage = new T [toSize];

				if (size >0) 
					memcpy(newStorage, localData, size * sizeof( T ) );
				
				delete [] localData;
				
				localData = newStorage;
				allocSize = toSize;
			 }
		 }
		 void shrink(){
			 if ( (size > 0) && (allocSize > size) ){
				T * newStorage = new T [size];
				
				memcpy(newStorage, localData, size * sizeof( T ) );

				delete [] localData;
				
				localData = newStorage;
				allocSize = size;
			 }
		 }

		 void insert(T & item){
			grow(size + 1);
			localData[size++] = item;	
		 }

};

template <class T> 
class fddStorage <T *>{
	private:
		size_t size;
		size_t * lineSizes;
		size_t allocSize;
		//size_t itemSize;

		T ** localData;
	public:
		fddStorage(){
			allocSize = 200;
			localData = new T*[allocSize];
			size = 0;
		}
		fddStorage(size_t s){
			allocSize = s;
			localData = new T*[s];
			lineSizes = new size_t[s];
			size = s;
		}
		fddStorage(T ** data, size_t * lineSizes, size_t s) : fddStorage(s){
			setData(data, lineSizes, s);
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( void * data, size_t s){
			std::cerr << "ERROR: Something went wrong in the code\n";
		}
		void setData( void ** data, size_t * lineSizes, size_t s){
			grow(s);
			for ( int i = 0; i < s; ++i){
				localData[i] = (T *) new  T [lineSizes[i]];
				memcpy(localData[i], data[i], lineSizes[i]*sizeof(T) );
			}
			size = s;
		}

		T ** getData(){ return localData; }
		size_t * getLineSizes(){ return lineSizes; }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }

		 T * & operator[](size_t ref){ return localData[ref]; }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) > toSize){
					toSize = allocSize * 1.8;
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
		 void shrink(){
			 if ( (size > 0) && (allocSize > size) ){
				T ** newStorage = new T* [size];
				
				memcpy(newStorage, localData, size * sizeof( T* ) );

				delete [] localData;
				
				localData = newStorage;
				allocSize = size;
			 }
		 }

		 void insert(T *& item, size_t s){
			grow(size + 1);
			lineSizes[size] = s;
			localData[size++] = item;	
		 }

};

template <> 
class fddStorage <void *>{
	private:
		size_t size;
		size_t * lineSizes;
		size_t allocSize;

		void ** localData;
	public:
		fddStorage(){
			allocSize = 200;
			localData = new void*[allocSize];
			size = 0;
		}
		fddStorage(size_t s){
			allocSize = s;
			localData = new void*[s];
			lineSizes = new size_t[s];
			size = s;
		}
		fddStorage(void ** data, size_t * lineSizes, size_t s) : fddStorage(s){
			setData(data, lineSizes, s);
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( void * data, size_t s){
			std::cerr << "ERROR: Something went wrong in the code\n";
		}
		void setData( void ** data, size_t * lineSizes, size_t s){
			grow(s);
			for ( size_t i = 0; i < s; ++i){
				localData[i] = (void *) new  char [lineSizes[i]];
				memcpy(localData[i], data[i], lineSizes[i] );
			}
			size = s;
		}

		void ** getData(){ return localData; }
		size_t * getLineSizes(){ return lineSizes; }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }

		 void * & operator[](size_t ref){ return localData[ref]; }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) > toSize){
					toSize = allocSize * 1.8;
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
		 void shrink(){
			 if ( (size > 0) && (allocSize > size) ){
				void ** newStorage = new void* [size];
				
				memcpy(newStorage, localData, size * sizeof( void* ) );

				delete [] localData;
				
				localData = newStorage;
				allocSize = size;
			 }
		 }

		 void insert(void *& item, size_t s){
			grow(size + 1);
			lineSizes[size] = s;
			localData[size++] = item;	
		 }

};

template <> 
class fddStorage <std::string> {
	private:
		size_t size;
		size_t allocSize;

		std::string * localData;
	public:
		fddStorage(){
			allocSize = 200;
			localData = new std::string[allocSize];
			size = 0;
		}
		fddStorage(size_t s){
			allocSize = s;
			localData = new std::string[s];
			size = s;
		}
		fddStorage(std::string * data, size_t s) : fddStorage(s){
			memcpy(localData, data, s*sizeof(std::string) );
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( void * data, size_t s){
			grow(s / sizeof(std::string));
			//memcpy(localData, data, s );
			for ( size_t i = 0; i < s; ++i){
				localData[i] = ((std::string*) data)[s];
			}
			size = s / sizeof(std::string);
		}
		void setData( void ** data, size_t * lineSizes, size_t s){
			std::cerr << "ERROR: Something went wrong in the code\n";
		}

		std::string * getData(){ return localData; }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }

		std::string & operator[](size_t ref){ return localData[ref]; }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) > toSize){
					toSize = allocSize * 1.8;
				}
				
				std::string * newStorage = new std::string [toSize];

				if (size >0) 
					memcpy(newStorage, localData, size * sizeof( std::string ) );
				
				delete [] localData;
				
				localData = newStorage;
				allocSize = toSize;
			 }
		 }
		 void shrink(){
			 if ( (size > 0) && (allocSize > size) ){
				 std::string * newStorage = new std::string [size];
				
				memcpy(newStorage, localData, size * sizeof( std::string ) );

				delete [] localData;
				
				localData = newStorage;
				allocSize = size;
			 }
		 }

		 void insert(std::string & item){
			grow(size + 1);
			localData[size++] = item;	
		 }

};



#endif
