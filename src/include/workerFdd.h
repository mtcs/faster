#ifndef LIBFASTER_WORKERFDD_H
#define LIBFASTER_WORKERFDD_H

#include <list>
#include <omp.h>
#include <unordered_map>

#include "workerFddBase.h"

namespace faster {
	template <class T>
	class fddStorage;

	template <class T>
	class workerFddCore;

	template <class T>
	class _workerFdd;

	template <class K, class T>
	class workerIFdd;

	class workerFddWrapper : public workerFddBase{
		protected:
			static std::unordered_map<fddType, int> hAssign; 
			static void * dLHandler[3];
			static std::unordered_map<char, void *> funcTable[3];

	};

	// Interface for dynamic loading that wraps C-style language
	//template <class T>
	class workerFdd : public workerFddWrapper{
		private:
			workerFddBase * _fdd;

			void loadLib(const fddType t);
			void loadSymbols(const fddType t);
		public:
			workerFdd(fddType t);
			workerFdd(unsigned int ident, fddType t);
			workerFdd(unsigned int ident, fddType t, size_t size);
			~workerFdd();

			fddType getType();
			fddType getKeyType();

			//T & operator[](size_t address);
			void * getItem(size_t address);
			//{
			//	return &(*this)[address];
			//}
			void * getKeys(){
				return NULL;
			}
			void * getData();
			size_t getSize();
			size_t itemSize();
			size_t baseSize();
			void deleteItem(void * item);
			void shrink();

			// For known types
			void setData(void * d , size_t size );
			void setData(void * d , size_t * lineSizes , size_t size );
			void setData(void * k  UNUSED, void * d  UNUSED, size_t size  UNUSED){}
			void setData(void * k  UNUSED, void * d  UNUSED, size_t *lineSizes  UNUSED, size_t size  UNUSED){}

			// For anonymous types
			void setDataRaw(void * data, size_t size) override;
			void setDataRaw(void * data, size_t *lineSizes, size_t size) override;
			void setDataRaw( void *k UNUSED, void *d UNUSED, size_t s UNUSED) {}
			void setDataRaw( void *k UNUSED, void *d UNUSED, size_t *l UNUSED, size_t s UNUSED) {}

			size_t * getLineSizes();

			void insert(void * in, size_t s);
			void insertl(void * in);

			//void insert(T & in);
			//void insert(T & in, size_t s);
			//void insert(std::list<T> & in);
			//void insert(std::list< std::pair<T, size_t> > & in);

			// Apply task functions to FDDs
			void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize);

			void collect(fastComm * comm) override;

			void groupByKey(fastComm *comm);
			void countByKey(fastComm *comm);
	};

}

#endif
