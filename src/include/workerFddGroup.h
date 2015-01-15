#ifndef LIBFASTER_WORKERFDDGROUP_H
#define LIBFASTER_WORKERFDDGROUP_H

#include <vector>
#include <string>

#include "workerFddBase.h"

namespace faster{

	template <typename K>
	class workerFddGroup : public workerFddBase{
		private:
			std::vector<workerFddBase*> members;
			std::vector<K> uKeys;

			/*template <typename T0, typename T1, typename T2>
			void decodeLast(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);
			template <typename T0, typename T1>
			inline void decodeThird(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);
			template <typename T0>
			inline void decodeSecond(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);// */

			std::vector< std::vector<void*>> findKeyInterval(size_t i);
			std::vector< std::vector<void*> > * getMemberKeyLocations(size_t i);


			//template <typename U, typename T0, typename T1, typename T2>
			template <typename U>
			void _apply(void * func, fddOpType op, workerFddBase * dest);
			//template <typename L, typename U, typename T0, typename T1, typename T2>
			template <typename L, typename U>
			void _applyI(void * func, fddOpType op, workerFddBase * dest);

			//template <typename T0, typename T1, typename T2>
			void _applyReduce(void * func, fddOpType op, fastCommBuffer & buffer);

			//template <typename T0, typename T1, typename T2>
			void _preApply(void * func, fddOpType op, workerFddBase * dest);
			//template <typename L, typename T0, typename T1, typename T2>
			template <typename L>
			void _preApplyI(void * func, fddOpType op, workerFddBase * dest);


			//template <typename U, typename T0, typename T1, typename T2>
			template <typename U>
			void  mapByKey(workerFddBase * dest, void * mapByKeyFunc);
			//template <typename L, typename U, typename T0, typename T1, typename T2>
			template <typename L, typename U>
			void mapByKeyI(workerFddBase * dest, void * mapByKeyFunc);

			//template <typename U, typename T0, typename T1, typename T2>
			template <typename U>
			void flatMapByKey(workerFddBase * dest, void * mapByKeyFunc);
			//template <typename L, typename U, typename T0, typename T1, typename T2>
			template <typename L, typename U>
			void flatMapByKeyI(workerFddBase * dest, void * mapByKeyFunc);

			template <typename U>
			void bulkFlatMap(workerFddBase * dest, void * mapByKeyFunc);
			template <typename L, typename U>
			void bulkFlatMapI(workerFddBase * dest, void * mapByKeyFunc);

			void updateByKey(void * mapByKeyFunc);
			void bulkUpdate(void * mapByKeyFunc);

			void exchangeDataByKey(fastComm *comm UNUSED, void * keyMap UNUSED){};
			std::vector< std::vector<void*> > * getKeyLocations(){ return NULL; }
			void cogroup(fastComm *comm);

		public:
			workerFddGroup(unsigned long int id, fddType keyT, std::vector<workerFddBase*> & members);

			fddType getType() { return Null; }
			fddType getKeyType() { return this->keyType; }

			void setData( void * d UNUSED, size_t s UNUSED) {}
			void setData( void * d UNUSED, size_t * ds UNUSED, size_t s UNUSED) {}
			void setData( void * k UNUSED, void * d UNUSED, size_t s UNUSED) {}
			void setData( void * k UNUSED, void * d UNUSED, size_t * ds UNUSED, size_t s UNUSED) {}

			void setDataRaw( void * d UNUSED, size_t s UNUSED) {}
			void setDataRaw( void * d UNUSED, size_t * ds UNUSED, size_t s UNUSED) {}
			void setDataRaw( void * k UNUSED, void * d UNUSED, size_t s UNUSED) {}
			void setDataRaw( void * k UNUSED, void * d UNUSED, size_t * ds UNUSED, size_t s UNUSED) {}

			void * getItem(size_t UNUSED p) { return NULL; }

			void * getKeys() { return NULL; }
			void * getData() { return NULL; }
			size_t getSize() { return 0; }
			size_t * getLineSizes() { return NULL; } 
			void   setSize(size_t s UNUSED) {}

			size_t itemSize() { return 0; }
			size_t baseSize() { return 0; }
 
			void deleteItem(void * item UNUSED) {}

			void shrink(){}
			void insertl(void * v UNUSED) {}
			void insert(void * k UNUSED, void * v UNUSED, size_t s UNUSED) {}

			void apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);
			void preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm) ;

			void collect(fastComm * comm UNUSED) { /* TODO */ }
	};

} 
#endif
