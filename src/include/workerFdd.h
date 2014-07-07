#ifndef LIBFASTER_WORKERFDD_H
#define LIBFASTER_WORKERFDD_H

#include <list>
#include <omp.h>

template <class T>
class fddStorage;

template <class T>
class workerFdd;

template <class K, class T>
class workerIFdd;

#include "workerFddBase.h"
#include "fddStorage.h"
#include "fastCommBuffer.h"

// Worker side FDD
template <class T>
class workerFdd : public workerFddBase{
	private:
		fddStorage <T> * localData;

		// Not Pointer -> Not Pointer
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename L, typename U>
		void _applyI(void * func, fddOpType op, workerIFdd<L,U> * dest);
		template <typename L, typename U>
		void _applyIP(void * func, fddOpType op, workerIFdd<L,U> * dest);
		void _applyReduce(void * func, fddOpType op, void *& result, size_t & rSize);

		template <typename L>
		void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
		void _preApply(void * func, fddOpType op, workerFddBase * dest);

		// --------- FUNCTIONS ----------

		template <typename U>
		void map (workerFdd<U> & dest, mapFunctionP<T,U> mapFunc);
		template <typename U>
		void map (workerFdd<U> & dest, PmapFunctionP<T,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, ImapFunctionP<T,L,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, IPmapFunctionP<T,L,U> mapFunc);

		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapFunctionP<T,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapFunctionP<T,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IbulkMapFunctionP<T,L,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IPbulkMapFunctionP<T,L,U> bulkMapFunc);

		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapFunctionP<T,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapFunctionP<T,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IflatMapFunctionP<T,L,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IPflatMapFunctionP<T,L,U> flatMapFunc );

		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapFunctionP<T,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapFunctionP<T,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc );




		// REDUCE
		T reduce (reduceFunctionP<T> reduceFunc);
		T bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc);


	public:

		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){
			localData = new fddStorage<T>();
		} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
			localData = new fddStorage<T>(size);
		}

		~workerFdd(){
			delete resultBuffer;
			delete localData;
		}

		// For known primitive types
		void setData(T * data, size_t size) {
			localData->setData(data, size);
		}
		// For anonymous primitive types
		void setData(void * data, size_t size) override{
			localData->setData(data, size);
		}
		void setData(void ** data UNUSED, size_t * listSizes UNUSED, size_t size UNUSED ) override{}
		void setData(void * keys UNUSED, void * data UNUSED, size_t size UNUSED) override{ }
		void setData(void * keys UNUSED, void ** data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override{ }

		fddType getType() override { return type; }
		fddType getKeyType() override { return Null; }

		T & operator[](size_t address){ return localData->getData()[address]; }
		void * getData() override{ return localData->getData(); }
		size_t getSize() override{ return localData->getSize(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T); }
		void deleteItem(void * item) override { delete (T*) item; }

		void insert(T & in){ localData->insert(in); }

		void insert(std::list<T> & in){ 
			typename std::list<T>::iterator it;

			if (localData->getSize() < in.size())
				localData->grow(in.size());

			for ( it = in.begin(); it != in.end(); it++)
				localData->insert(*it); 
		}

		void shrink(){ localData->shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize);

};

// Pointer specialization
template <class T>
class workerFdd<T *> : public workerFddBase{
	private:
		fddStorage <T *> * localData;

		// Pointer -> Not Pointer
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename L, typename U>
		void _applyI(void * func, fddOpType op, workerIFdd<L,U> * dest);
		template <typename L, typename U>
		void _applyIP(void * func, fddOpType op, workerIFdd<L,U> * dest);
		void _applyReduce(void * func, fddOpType op, void *& result, size_t & rSize);

		template <typename L>
		void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
		void _preApply(void * func, fddOpType op, workerFddBase * dest);
		// --------- FUNCTIONS ----------

		template <typename U>
		void map (workerFdd<U> & dest, mapPFunctionP<T,U> mapFunc);
		template <typename U>
		void map (workerFdd<U> & dest, PmapPFunctionP<T,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, ImapPFunctionP<T,L,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, IPmapPFunctionP<T,L,U> mapFunc);

		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapPFunctionP<T,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapPFunctionP<T,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IbulkMapPFunctionP<T,L,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IPbulkMapPFunctionP<T,L,U> bulkMapFunc);

		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapPFunctionP<T,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapPFunctionP<T,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IflatMapPFunctionP<T,L,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IPflatMapPFunctionP<T,L,U> flatMapFunc );

		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapPFunctionP<T,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc );




		// REDUCE
		std::pair<T*,size_t> reduce (PreducePFunctionP<T> reduceFunc);
		std::pair<T*,size_t> bulkReduce (PbulkReducePFunctionP<T> bulkReduceFunc);
		

	public:
		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){
			localData = new fddStorage<T*>();
		} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
			localData = new fddStorage<T*>(size);
		}

		~workerFdd(){
			delete localData;
			delete resultBuffer;
		}


		void setData(T ** data, size_t *lineSizes, size_t size) {
			localData->setData(data, lineSizes, size);
		}
		void setData(void * data UNUSED, size_t size UNUSED) override{ }
		void setData(void ** data, size_t *lineSizes, size_t size) override{
			localData->setData((T**) data, lineSizes, size);
		}
		void setData(void * keys UNUSED, void * data UNUSED, size_t size UNUSED) override{ }
		void setData(void * keys UNUSED, void ** data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override{ }

		fddType getType() override { return type; }
		fddType getKeyType() override { return Null; }
		T *& operator[](size_t address){ return localData->getData()[address]; }
		void * getData() override{ return localData->getData(); }
		size_t getSize() override{ return localData->getSize(); }
		size_t * getLineSizes(){ return localData->getLineSizes(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T*); }
		void deleteItem(void * item) override { delete (T*) item; }

		void insert(T* & in, size_t s){ localData->insert(in, s); }

		void insert(std::list< std::pair<T*, size_t> > & in){ 
			typename std::list< std::pair<T*, size_t> >::iterator it;

			if (localData->getSize() < in.size())
				localData->grow(in.size());

			for ( it = in.begin(); it != in.end(); it++)
				localData->insert(it->first, it->second); 
		}

		void shrink(){ localData->shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize);
};

#endif
