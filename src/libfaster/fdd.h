#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>

#include "context.h"


#define FDD_TYPE_INT 		0x01
#define FDD_TYPE_LONGINT 	0x02 
#define FDD_TYPE_FLOAT 		0x04
#define FDD_TYPE_DOUBLE 	0x08
#define FDD_TYPE_STRING 	0x10

#define FDD_FUNC_MAP		0x01
#define FDD_FUNC_REDUCE		0x02


template <class T>
class fdd {
	public:
		//fdd(fastContext &context){
		//	type = typeid(T);
			//id = context.createFDD(type);
		//}
		~fdd(){
			context.destroyFDD(id);
		}

		template <typename U>
		fdd<U> * map( U (*mapFunction) (T &) ){

			std::vector <long> computeIndex = context.requestDataIndexes(id, resultId, FDD_FUNC_MAP);

			for (i = 0; i < localDataIndex.length(); i++ ){
				if (localDataIndex[i] == computeIndex[j]){
					j++;
					mapFunction( (int) this.localData[j]);
				}
			}
		}

	private:
		unsigned long int id;
		size_t size;
		char type;
		fastContext context;

		std::vector <T> localData;
		std::vector <long> localDataIndex;
};

template <> fdd::fdd(fastContext &context) <int> {
	type = FDD_TYPE_INT;
	id = context.createFDD(type);
};

#endif
