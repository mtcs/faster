#ifndef LIBFASTER_FDDSTORAGE_H
#define LIBFASTER_FDDSTORAGE_H

template <class T> class fddStorage;

#include "misc.h"


// FDD storage place
// Stores worker's FDD data locally
template <class T> 
class fddStorage {
	public:
		fddStorage() {
			type = detectType( typeid(T).name() );
			localData = NULL;
		}

		fddStorage(T * data, size_t s, unsigned int lowId, unsigned int highId) : fddStorage(){
			setData(data, s, lowId, highId);
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( T * data, size_t s, unsigned int lowId, unsigned int highId){
			localData = new T[s];
			memcpy(localData, data, s * sizeof ( T ) );
			size = s;
			dataRange.first = lowId;
			dataRange.second = highId;
		}

		void setOwnership(unsigned int f, unsigned int s){
			ownership.first = f;
			ownership.second = s;
		}

	private:
		fddType type;
		size_t size;

		T * localData;
		std::pair<unsigned int, unsigned int> dataRange;
		std::pair<unsigned int, unsigned int> ownership;
};


#endif
