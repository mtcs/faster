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

		T * getData(){ return localData; }
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


#endif
