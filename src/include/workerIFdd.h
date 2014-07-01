#ifndef LIBFASTER_WORKERIFDD_H
#define LIBFASTER_WORKERIFDD_H

#include <list>
#include <tuple>
#include <omp.h>

template <class T>
class workerFdd ;

template <class K, class T>
class workerIFdd ;

template <class K, class T> 
class indexedFddStorage;

#include "workerFddBase.h"
#include "indexedFddStorage.h"


// Worker side FDD
template <class K, class T>
class workerIFdd : public workerFddBase{
	private:
		indexedFddStorage <K,T> * localData;

		template <typename L, typename U>
		void _applyIMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIPMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _applyMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyPMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename L, typename U>

		void _applyIFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIPFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _applyFlatMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyPFlatMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>

		void _applyReduce(void * func, fddOpType op, workerFdd<U> * dest);

		template <typename L>
		void _preApplyIMap(void * func, fddOpType op, workerFddBase * destze);
		void _preApplyMap(void * func, fddOpType op, workerFddBase * destze);
		template <typename L>
		void _preApplyIFlatMap(void * func, fddOpType op, workerFddBase * destze);
		void _preApplyFlatMap(void * func, fddOpType op, workerFddBase * destze);

		void applyMap(void * func, fddOpType op, workerFddBase * destze);
		void applyFlatMap(void * func, fddOpType op, workerFddBase * destze);
		void applyReduce(void * func, fddOpType op, void * result, size_t & rSize);

		// --------- FUNCTIONS ----------

		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, ImapIFunctionP<K,T,L,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, IPmapIFunctionP<K,T,L,U> mapFunc);
		template < typename U>
		void map (workerFdd<U> & dest, mapIFunctionP<K,T,U> mapFunc);
		template <typename U>
		void map (workerFdd<U> & dest, PmapIFunctionP<K,T,U> mapFunc);


		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IbulkMapIFunctionP<K,T,L,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IPbulkMapIFunctionP<K,T,L,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapIFunctionP<K,T,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapIFunctionP<K,T,U> bulkMapFunc);


		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IflatMapIFunctionP<K,T,L,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IPflatMapIFunctionP<K,T,L,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapIFunctionP<K,T,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapIFunctionP<K,T,U> flatMapFunc );


		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc );


		// REDUCE
		std::pair<K,T> reduce (IreduceIFunctionP<K,T> reduceFunc);
		std::pair<K,T> bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc);


	public:
		workerIFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerIFdd(unsigned int ident, fddType t, size_t size) : workerIFdd(ident, t){ 
			localData = new indexedFddStorage<K,T>(size);
		}

		~workerIFdd(){
			delete localData;
		}

		void setData(K * keys, T * data, size_t size) {
			localData->setData( keys, data, size);
		}
		void setData(void * data, size_t size) override{}
		void setData(void ** data, size_t *lineSizes, size_t size) override{ }
		void setData(void * keys, void * data, size_t size) override{
			localData->setData((K*) keys, (T*) data, size);
		}
		void setData(void * keys, void ** data, size_t *lineSizes, size_t size) override{ }
		fddType getType() override { return type; }
		fddType getKeyType() override { return keyType; }

		T & operator[](size_t address){ return localData->getData()[address]; }
		void * getData() override{ return localData->getData(); }
		K * getKeys(){ return localData->getKeys(); }
		size_t getSize() override{ return localData->getSize(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T); }

		void insert(K key, T & in);
		void insert(std::list< std::pair<K, T> > & in);


		void shrink(){ localData->shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize);

};

// Pointer specialization
template <class K, class T>
class workerIFdd<K,T*> : public workerFddBase{
	private:
		indexedFddStorage <K,T*> * localData;

		template <typename L, typename U>
		void _applyIMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIPMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _applyMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyPMap(void * func, fddOpType op, workerFdd<U> * dest);

		template <typename L, typename U>
		void _applyIFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIPFlatMap(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _applyFlatMap(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyPFlatMap(void * func, fddOpType op, workerFdd<U> * dest);

		template <typename L>
		void _preApplyIMap(void * func, fddOpType op, workerFddBase * dest);
		void _preApplyMap(void * func, fddOpType op, workerFddBase * dest);
		template <typename L>
		void _preApplyIFlatMap(void * func, fddOpType op, workerFddBase * dest);
		void _preApplyFlatMap(void * func, fddOpType op, workerFddBase * dest);

		void applyMap(void * func, fddOpType op, workerFddBase * dest);
		void applyFlatMap(void * func, fddOpType op, workerFddBase * dest);
		void applyReduce(void * func, fddOpType op, void * result, size_t & rSize);

		// --------- FUNCTIONS ----------

		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, ImapIPFunctionP<K,T,L,U> mapFunc);
		template <typename L, typename U>
		void map (workerIFdd<L,U> & dest, IPmapIPFunctionP<K,T,L,U> mapFunc);
		template <typename U>
		void map (workerFdd<U> & dest, mapIPFunctionP<K,T,U> mapFunc);
		template <typename U>
		void map (workerFdd<U> & dest, PmapIPFunctionP<K,T,U> mapFunc);


		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IbulkMapIPFunctionP<K,T,L,U> bulkMapFunc);
		template <typename L, typename U>
		void bulkMap (workerIFdd<L,U> & dest, IPbulkMapIPFunctionP<K,T,L,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapIPFunctionP<K,T,U> bulkMapFunc);
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapIPFunctionP<K,T,U> bulkMapFunc);


		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IflatMapIPFunctionP<K,T,L,U> flatMapFunc );
		template <typename L, typename U>
		void flatMap(workerIFdd<L,U> & dest,  IPflatMapIPFunctionP<K,T,L,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapIPFunctionP<K,T,U> flatMapFunc );
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapIPFunctionP<K,T,U> flatMapFunc );


		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc );
		template <typename L, typename U>
		void bulkFlatMap(workerIFdd<L,U> & dest,  IPbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc );
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc );


		// REDUCE
		std::tuple<K,T*,size_t> reduce (size_t & rSize, IPreduceIPFunctionP<K,T> reduceFunc);

		std::tuple<K,T*,size_t> bulkReduce (size_t & rSize, IPbulkReduceIPFunctionP<K,T> bulkReduceFunc);
		

	public:
		workerIFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerIFdd(unsigned int ident, fddType t, size_t size) : workerIFdd(ident, t){ 
			localData = new indexedFddStorage<K,T*>(size);
		}

		~workerIFdd(){
			delete localData;
		}


		void setData(K * keys, T ** data, size_t *lineSizes, size_t size){
			localData->setData(keys, data, lineSizes, size);
		}
		void setData(void * data, size_t size) override{ }
		void setData(void ** data, size_t *lineSizes, size_t size) override{}
		void setData(void * keys, void * data, size_t size) override{ }
		void setData(void * keys, void ** data, size_t *lineSizes, size_t size) override{
			localData->setData((K*) keys, (T**) data, lineSizes, size);
		}
		fddType getType() override { return type; }
		fddType getKeyType() override { return keyType; }

		T *& operator[](size_t address){ return localData->getData()[address]; }
		void * getData() override{ return localData->getData(); }
		K * getKeys() { return localData->getKeys(); }
		size_t getSize() override{ return localData->getSize(); }
		size_t * getLineSizes(){ return localData->getLineSizes(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T*); }

		void insert(K key, T* & in, size_t s);
		void insert(std::list< std::tuple<K, T*, size_t> > & in);

		void shrink(){ localData->shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize);
};



#endif
