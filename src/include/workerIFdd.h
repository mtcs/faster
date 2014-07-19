#ifndef LIBFASTER_WORKERIFDD_H
#define LIBFASTER_WORKERIFDD_H

#include <list>
#include <tuple>
#include <omp.h>
#include <set>

template <class T>
class workerFdd ;

template <class K, class T>
class workerIFdd ;

template <class K, class T> 
class indexedFddStorage;

#include "workerFddBase.h"

template<typename K, typename T>
class workerIFddCore : public workerFddBase{
	protected:
		indexedFddStorage <K,T> * localData;
		bool groupedByKey;

		// ByKey Functions
		K * distributeOwnership(fastComm * comm, K * uKeys, size_t cSize);
		void sendPartKeyCount(fastComm *comm);
		CountKeyMapT<K> recvPartKeyMaxCount(fastComm *comm, PPCountKeyMapT<K> & keyPPMaxCount);
		CountKeyMapT<K> recvPartKeyCount(fastComm *comm);
		CountKeyMapT<K> distributedMaxKeyCount(fastComm *comm, PPCountKeyMapT<K> & keyPPMaxCount);

	public:
		workerIFddCore(unsigned int ident, fddType t);
		workerIFddCore(unsigned int ident, fddType t, size_t size);
		~workerIFddCore();
		
		// ByKey Functions
		void groupByKey(fastComm *comm) override;
		void countByKey(fastComm *comm) override;

		fddType getType() override ;
		fddType getKeyType() override ;

		void setDataRaw(void * data UNUSED, size_t size UNUSED) override {}
		void setDataRaw(void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override {}

		T & operator[](size_t address);
		void * getData() override;
		K * getKeys();
		size_t getSize() override;
		size_t itemSize() override;
		size_t baseSize() override;
		void deleteItem(void * item) override ;
		void shrink();

};

// Worker side FDD
template <class K, class T>
class workerIFdd : public workerIFddCore<K,T>{
	private:
		// Procedures that apply the FDD core functions
		template <typename L, typename U>
		void _applyI(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIP(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest);

		template <typename L>
		void _preApplyI(void * func, fddOpType op, workerFddBase * destze);
		void _preApply(void * func, fddOpType op, workerFddBase * destze);

		void applyDependent(void * func, fddOpType op, workerFddBase * destze);
		void applyIndependent(void * func, fddOpType op, void *& result, size_t & rSize);

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

		// ByKey Functions


	public:
		workerIFdd(unsigned int ident, fddType t) : workerIFddCore<K,T>(ident, t) {}
		workerIFdd(unsigned int ident, fddType t, size_t size) : workerIFddCore<K,T>(ident, t, size) {}
		~workerIFdd(){}

		// For known types
		void setData(K * keys, T * data, size_t size) ;
		
		// For anonymous types
		void setDataRaw(void * keys, void * data, size_t size) override;
		void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override{}

		void insert(K key, T & in);
		void insert(std::list< std::pair<K, T> > & in);


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize);

		void collect(fastComm * comm) override;

};

// Pointer specialization
template <class K, class T>
class workerIFdd<K,T*> : public workerIFddCore<K,T*>{
	private:
		// Procedures that apply the FDD core functions
		template <typename L, typename U>
		void _applyI(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename L, typename U>
		void _applyIP(void * func, fddOpType op, workerIFdd<L, U> * dest);
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest);
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest);

		template <typename L>
		void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
		void _preApply(void * func, fddOpType op, workerFddBase * dest);

		void applyDependent(void * func, fddOpType op, workerFddBase * dest);
		void applyIndependent(void * func, fddOpType op, void *& result, size_t & rSize);

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
		std::tuple<K,T*,size_t> reduce (IPreduceIPFunctionP<K,T> reduceFunc);

		std::tuple<K,T*,size_t> bulkReduce (IPbulkReduceIPFunctionP<K,T> bulkReduceFunc);
		

	public:
		workerIFdd(unsigned int ident, fddType t) : workerIFddCore<K,T*>(ident, t) {}
		workerIFdd(unsigned int ident, fddType t, size_t size) : workerIFddCore<K,T*>(ident, t, size) {}
		~workerIFdd(){}


		// For known types
		void setData(K * keys, T ** data, size_t *lineSizes, size_t size);
		
		// For anonymous types
		void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t size UNUSED) override{}
		void setDataRaw(void * keys, void * data, size_t *lineSizes, size_t size) override;

		size_t * getLineSizes();

		void insert(K key, T* & in, size_t s);
		void insert(std::list< std::tuple<K, T*, size_t> > & in);


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize);

		void collect(fastComm * comm) override;

};



#endif
