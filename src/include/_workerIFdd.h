#ifndef LIBFASTER_WORKERIFDD_H
#define LIBFASTER_WORKERIFDD_H

#include <list>
#include <tuple>
#include <omp.h>
#include <set>
#include <unordered_map>
#include <iostream>
#include <memory>

#include "workerFddBase.h"

namespace faster{

	template <class T>
	class workerFdd ;

	template <class K, class T>
	class workerIFdd ;

	template <class K, class T>
	class indexedFddStorage;

	template<typename K, typename T>
	class workerIFddCore : public workerFddBase{
		protected:
			indexedFddStorage <K,T> * localData;
			std::shared_ptr<std::vector<K>> uKeys;
			std::shared_ptr<std::unordered_map<K, int>> keyMap;
			std::vector< std::vector<void*> > keyLocations;
			bool groupedByKey;
			bool groupedByHash;

			// ByKey Functions
			K * distributeOwnership(fastComm * comm, K * uKeys, size_t cSize);
			void sendPartKeyCount(fastComm *comm);
			std::unordered_map<K, size_t> recvPartKeyMaxCount(fastComm *comm, std::unordered_map<K, std::pair<size_t, std::deque<int>> > & keyPPMaxCount);
			std::unordered_map<K, size_t> recvPartKeyCount(fastComm *comm);
			std::unordered_map<K, size_t> distributedMaxKeyCount(fastComm *comm, std::unordered_map<K, std::pair<size_t, std::deque<int>> > & keyPPMaxCount);

			// Exchange Data By Key private functions
			inline bool EDBKsendDataAsync(
					fastComm *comm,
					int owner,
					K & key,
					T & data,
					std::vector<size_t> & dataSize);
			void flushDataSend(
					fastComm *comm,
					std::vector<size_t> & dataSize);
			bool EDBKSendData(
					fastComm *comm,
					std::vector<size_t> & dataSize);
			bool EDBKSendDataHashed(
					fastComm *comm,
					size_t & pos,
					std::vector<bool> & deleted,
					bool & dirty
					);
			inline bool tryInsert( K * keys,
					T * data,
					K & insertKey,
					T & insertData,
					size_t & pos,
					size_t & posLimit,
					std::vector<bool> & deleted
				      );
			size_t EDBKRecvData(fastComm *comm,
					size_t & posLimit,
					std::vector<bool> & deleted,
					std::vector< std::pair<K,T> >  & recvData,
					int & peersFinised,
					bool & dirty
					);
			void EDBKFinishDataInsert(std::vector<bool> & deleted, std::vector< std::pair<K,T> >  & recvData, size_t & pos );
			void EDBKShrinkData(std::vector<bool> & deleted, size_t & pos);

			void findMyKeys(int numProcs, int Id);
			void findMyKeysByHash(int numProcs);

		public:
			workerIFddCore(unsigned int ident, fddType kt, fddType t);
			workerIFddCore(unsigned int ident, fddType kt, fddType t, size_t size);
			virtual ~workerIFddCore();

			fddType getType() override ;
			fddType getKeyType() override ;

			void setData(void * data UNUSED, size_t size UNUSED){}
			void setData(void * data UNUSED, size_t  * ls UNUSED, size_t size UNUSED){}
			void setDataRaw(void * data UNUSED, size_t size UNUSED) override {}
			void setDataRaw(void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override {}

			T & operator[](size_t address);
			void * getItem(size_t address){
				return &localData[address];
			}
			void * getData() override;
			void * getKeys();
			size_t getSize() override;
			size_t itemSize() override;
			size_t baseSize() override;
			void   setSize(size_t s);

			void deleteItem(void * item) override ;
			void shrink();

			std::vector< std::vector<T*> > findKeyInterval(K * keys, T * data, size_t fddSize);

			void preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm);

			bool onlineReadStage3(std::deque<std::vector<std::pair<K,T>>> & q2, omp_lock_t & q2lock);
			bool onlinePartReadStage3(std::unordered_map<K, int> & localKeyMap, fastComm *comm, void * funcP, std::deque<std::vector<std::pair<K,T>>> & q2, omp_lock_t & q2lock);
			void onlineFullPartRead(fastComm *comm, void * funcP);
			void onlinePartRead(fastComm *comm, void * funcP);
			void onlineRead(fastComm *comm);

			// ByKey Functions
			void groupByKey(fastComm *comm);
			void groupByKeyHashed(fastComm *comm);
			void countByKey(fastComm *comm);
			void exchangeDataByKey(fastComm *comm);
			bool exchangeDataByKeyHashed(fastComm *comm);
			void exchangeDataByKeyMapped(fastComm *comm);
			std::vector< std::vector<void*> > * getKeyLocations(){ return  &keyLocations; }
			void * getUKeys(){ return &uKeys; }
			void  setUKeys(void * uk){ uKeys = * (std::shared_ptr<std::vector<K>>*) uk; }
			void * getKeyMap(){ return &keyMap; }
			void  setKeyMap(void * km){ keyMap = * (std::shared_ptr<std::unordered_map<K, int>>*) km; }

			void writeToFile(void * path, size_t procId, void * sufix);



	};

	// Worker side FDD
	template <class K, class T>
	class _workerIFdd : public workerIFddCore<K,T>{
		private:
			// Procedures that apply the FDD core functions
			template <typename L, typename U>
			void _applyI(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyIP(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _apply(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _applyP(void * func, fddOpType op, workerFddBase * dest);

			template <typename L>
			void _preApplyI(void * func, fddOpType op, workerFddBase * destze);
			void _preApply(void * func, fddOpType op, workerFddBase * destze);

			void applyDependent(void * func, fddOpType op, workerFddBase * destze);
			void applyIndependent(void * func, fddOpType op, fastCommBuffer & buffer);

			// --------- FUNCTIONS ----------

			template <typename L, typename U>
			void map (workerFddBase * dest, ImapIFunctionP<K,T,L,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, IPmapIFunctionP<K,T,L,U> mapFunc);
			template < typename U>
			void map (workerFddBase * dest, mapIFunctionP<K,T,U> mapFunc);
			template <typename U>
			void map (workerFddBase * dest, PmapIFunctionP<K,T,U> mapFunc);

			template <typename L, typename U>
			void mapByKey (workerFddBase * dest, ImapByKeyIFunctionP<K,T,L,U> mapByKeyFunc);
			template <typename L, typename U>
			void mapByKey (workerFddBase * dest, IPmapByKeyIFunctionP<K,T,L,U> mapByKeyFunc);
			template < typename U>
			void mapByKey (workerFddBase * dest, mapByKeyIFunctionP<K,T,U> mapByKeyFunc);
			template <typename U>
			void mapByKey (workerFddBase * dest, PmapByKeyIFunctionP<K,T,U> mapByKeyFunc);


			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IbulkMapIFunctionP<K,T,L,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IPbulkMapIFunctionP<K,T,L,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, bulkMapIFunctionP<K,T,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, PbulkMapIFunctionP<K,T,U> bulkMapFunc);


			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IflatMapIFunctionP<K,T,L,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IPflatMapIFunctionP<K,T,L,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  flatMapIFunctionP<K,T,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  PflatMapIFunctionP<K,T,U> flatMapFunc );


			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapIFunctionP<K,T,L,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  bulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  PbulkFlatMapIFunctionP<K,T,U> bulkFlatMapFunc );


			// Out put independent
			// UPDATE
			char update (updateIFunctionP<K,T> reduceFunc);
			// REDUCE
			std::pair<K,T> reduce (IreduceIFunctionP<K,T> reduceFunc);
			std::pair<K,T> bulkReduce (IbulkReduceIFunctionP<K,T> bulkReduceFunc);

			// ByKey Functions


		public:
			_workerIFdd(unsigned int ident, fddType kt, fddType t) : workerIFddCore<K,T>(ident, kt, t) {}
			_workerIFdd(unsigned int ident, fddType kt, fddType t, size_t size) : workerIFddCore<K,T>(ident, kt, t, size) {}
			~_workerIFdd(){
			}

			// For known types
			void setData(K * keys, T * data, size_t size) ;
			void setData(void * keys, void * data, size_t size){
				setData( (K*) keys, (T*) data, size);
			}
			void setData(void * keys, void * data, size_t * lineSizes UNUSED, size_t size){
				setData( (K*) keys, (T*) data, size);
			}

			// For anonymous types
			void setDataRaw(void * keys, void * data, size_t size) override;
			void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override{}

			size_t * getLineSizes(){
				return NULL;
			}

			void insert(void * k, void * in, size_t s);
			void insertl(void * in);

			void insert(K & key, T & in);
			void insert(std::deque< std::pair<K, T> > & in);


			// Apply task functions to FDDs
			void apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);

			void collect(fastComm * comm) override;

	};

	// Pointer specialization
	template <class K, class T>
	class _workerIFdd<K,T*> : public workerIFddCore<K,T*>{
		private:
			// Procedures that apply the FDD core functions
			template <typename L, typename U>
			void _applyI(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyIP(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _apply(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _applyP(void * func, fddOpType op, workerFddBase * dest);

			template <typename L>
			void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
			void _preApply(void * func, fddOpType op, workerFddBase * dest);

			void applyDependent(void * func, fddOpType op, workerFddBase * dest);
			void applyIndependent(void * func, fddOpType op, fastCommBuffer & buffer);

			// --------- FUNCTIONS ----------

			template <typename L, typename U>
			void map (workerFddBase * dest, ImapIPFunctionP<K,T,L,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, IPmapIPFunctionP<K,T,L,U> mapFunc);
			template <typename U>
			void map (workerFddBase * dest, mapIPFunctionP<K,T,U> mapFunc);
			template <typename U>
			void map (workerFddBase * dest, PmapIPFunctionP<K,T,U> mapFunc);

			template <typename L, typename U>
			void mapByKey (workerFddBase * dest, ImapByKeyIPFunctionP<K,T,L,U> mapByKeyFunc);
			template <typename L, typename U>
			void mapByKey (workerFddBase * dest, IPmapByKeyIPFunctionP<K,T,L,U> mapByKeyFunc);
			template <typename U>
			void mapByKey (workerFddBase * dest, mapByKeyIPFunctionP<K,T,U> mapByKeyFunc);
			template <typename U>
			void mapByKey (workerFddBase * dest, PmapByKeyIPFunctionP<K,T,U> mapByKeyFunc);


			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IbulkMapIPFunctionP<K,T,L,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IPbulkMapIPFunctionP<K,T,L,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, bulkMapIPFunctionP<K,T,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, PbulkMapIPFunctionP<K,T,U> bulkMapFunc);


			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IflatMapIPFunctionP<K,T,L,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IPflatMapIPFunctionP<K,T,L,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  flatMapIPFunctionP<K,T,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  PflatMapIPFunctionP<K,T,U> flatMapFunc );


			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapIPFunctionP<K,T,L,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  bulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  PbulkFlatMapIPFunctionP<K,T,U> bulkFlatMapFunc );


			// REDUCE
			std::tuple<K,T*,size_t> reduce (IPreduceIPFunctionP<K,T> reduceFunc);

			std::tuple<K,T*,size_t> bulkReduce (IPbulkReduceIPFunctionP<K,T> bulkReduceFunc);


		public:
			_workerIFdd(unsigned int ident, fddType kt, fddType t) : workerIFddCore<K,T*>(ident, kt, t) {}
			_workerIFdd(unsigned int ident, fddType kt, fddType t, size_t size) : workerIFddCore<K,T*>(ident, kt, t, size) {}
			~_workerIFdd(){}


			// For known types
			void setData(K * keys, T ** data, size_t *lineSizes, size_t size);

			void setData(void * keys UNUSED, void * data UNUSED, size_t size UNUSED){ }
			void setData(void * keys, void * data, size_t *lineSizes, size_t size){
				setData( (K*) keys, (T**) data, lineSizes, size);
			}

			// For anonymous types
			void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t size UNUSED) override{}
			void setDataRaw(void * keys, void * data, size_t *lineSizes, size_t size) override;

			size_t * getLineSizes();

			void insert(void * k, void * in, size_t s);
			void insertl(void * in);

			void insert(K & key, T* & in, size_t s);
			void insert(std::deque< std::tuple<K, T*, size_t> > & in);


			// Apply task functions to FDDs
			void apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);

			void collect(fastComm * comm) override;

	};

}

#endif
