#ifndef LIBFASTER_FDDSTORAGE_H
#define LIBFASTER_FDDSTORAGE_H

template <class T> class fddStorage;

#include "misc.h"


// FDD storage place
// Stores worker's FDD data locally
template <class T> 
class fddStorage {
	public:
		fddStorage(){
			size = 0;
			allocSize = 200;
			localData = new T[allocSize];
		}
		fddStorage(T * data, size_t s, unsigned int lowId, unsigned int highId) : fddStorage(){
			setData(data, s, lowId, highId);
			allocSize = s;
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( T * data, size_t s){
			grow(s);
			memcpy(localData, data, s * sizeof ( T ) );
			size = s;
			allocSize = s;
		}

		T * getData(){
			return localData;
		}

		size_t getSize(){
			return size;
		}

		 T & operator[](size_t ref){
			 return localData[ref];
		 }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) < toSize){
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
			grow(allocSize + 1);
			localData[size++] = item;	
		 }

	private:
		size_t size;
		size_t allocSize;

		T * localData;
};


#endif
