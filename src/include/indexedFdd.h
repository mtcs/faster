#ifndef LIBFASTER_INDEXEDFDD_H
#define LIBFASTER_INDEXEDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>
#include <tuple>
#include <memory>

#include "definitions.h"
#include "fddBase.h"
#include "fastContext.h"
#include "misc.h"

namespace faster{

	class fastContext;

	template<typename K> 
	class groupedFdd;

	template <typename K, typename T> 
	class indexedFdd ; 

	// Driver side FDD
	// It just sends commands to the workers.
	template <typename K, typename T> 
	class iFddCore : public fddBase{

		protected:
			bool groupedByKey;
			bool groupedByMap;
			fastContext * context;

			iFddCore() {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
				groupedByMap = false;
				cached = false;
			}
			
			// Create a empty fdd
			iFddCore(fastContext &c) {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
				groupedByMap = false;
				cached = false;
				context = &c;
			}

			// Create a empty fdd with a pre allocated size
			iFddCore(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
				groupedByMap = false;
				cached = false;
				context = &c;
				this->size = s;
				this->dataAlloc = dataAlloc;
			}

			virtual ~iFddCore(){
			}

			std::unordered_map<K, std::tuple<size_t, int, size_t>> * calculateKeyCount(std::vector< std::pair<void *, size_t> > & result);
			std::unordered_map<K, int> calculateKeyMap(std::unordered_map<K, std::tuple<size_t, int, size_t>> & count);

			// -------------- Core FDD Functions --------------- //
			void update(void * funcP, fddOpType op);
			fddBase * _map(void * funcP, fddOpType op, fddBase * newFdd, system_clock::time_point & start);
			template <typename U> 
			fdd<U> * map( void * funcP, fddOpType op);
			template <typename L, typename U> 
			indexedFdd<L,U> * mapI( void * funcP, fddOpType op);

			indexedFdd<K,T> * groupByKeyMapped();
			indexedFdd<K,T> * groupByKeyHashed();

		public:
			//template<typename... FddTypes, typename... Args> 
			//groupedFdd<K, T, FddTypes...> * cogroup(Args * ... args){
				//return new groupedFdd<K, T, FddTypes...>(context, this, args...);
			//}
			template<typename U> 
			groupedFdd<K> * cogroup(iFddCore<K,U> * fdd1){

				this->groupByKey();
				auto start = system_clock::now();

				return new groupedFdd<K>(context, this, fdd1, start);
			}

			template<typename U, typename V> 
			groupedFdd<K> * cogroup(iFddCore<K,U> * fdd1, iFddCore<K,V> * fdd2){

				this->groupByKey();
				auto start = system_clock::now();

				return new groupedFdd<K>(context, this, fdd1, fdd2, start);
			}

			std::unordered_map<K, size_t> countByKey();

			indexedFdd<K,T> * groupByKey();

			void discard(){
				//std::cerr << "\033[0;31mDEL" << id << "\033[0m  "; 
				context->discardFDD(id);
			}

			void writeToFile(std::string path, std::string sufix);

			bool isGroupedByKey() { 
				return groupedByKey; 
			}
			void setGroupedByKey(bool gbk) {
				groupedByKey = gbk;
			}
			void setGroupedByMap(bool gbm) {
				groupedByMap = gbm;
			}

	};

	template <typename K, typename T> 
	class indexedFdd : public iFddCore<K,T>{
		private:
			std::pair <K,T> finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op);
			std::pair <K,T> reduce( void * funcP, fddOpType op);



		public:
			// -------------- Constructors --------------- //

			// Create a empty fdd
			indexedFdd(fastContext &c) : iFddCore<K,T>(c){ 
				this->_tType = decodeType(typeid(T).hash_code());
				this->id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code());
			}

			// Create a empty fdd with a pre allocated size
			indexedFdd(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) : iFddCore<K,T>(c, s, dataAlloc){ 
				this->_tType = decodeType(typeid(T).hash_code());
				this->id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), dataAlloc);
			}
			indexedFdd(fastContext &c, size_t s) : indexedFdd(c, s, c.getAllocation(s)) { }

			// Create a fdd from a array in memory
			indexedFdd(fastContext &c, K * keys, T * data, size_t size) : indexedFdd(c, size){
				c.parallelizeI(this->id, keys, data, size);
			}

			~indexedFdd(){
			}

			// -------------- FDD Functions --------------- //

			//  Update
			indexedFdd<K,T> * update(updateIFunctionP<K,T> funcP){
				iFddCore<K,T>::_update((void*) funcP, OP_Update);
				return this;
			}
			// Map
			template <typename L, typename U> 
			indexedFdd<L,U> * map( ImapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( IPmapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			fdd<U> * map( mapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			fdd<U> * map( PmapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_Map);
			}

			// MapByKey
			template <typename L, typename U> 
			indexedFdd<L,U> * mapByKey( ImapByKeyIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * mapByKey( IPmapByKeyIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			fdd<U> * mapByKey( mapByKeyIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			fdd<U> * mapByKey( PmapByKeyIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_MapByKey);
			}


			// BulkMap
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IbulkMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IPbulkMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkMap( bulkMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkMap( PbulkMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkMap);
			}


			// FlatMap
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IflatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IPflatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * flatMap( flatMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * flatMap( PflatMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_FlatMap);
			}


			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkFlatMap( bulkFlatMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkFlatMap( PbulkFlatMapIFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}

			// ------------------ Reduce ----------------- //

			// Run a Reduce
			std::pair<K,T> reduce( IreduceIFunctionP<K,T> funcP ){
				return reduce((void*) funcP, OP_Reduce);
			}
			//indexedFdd<K,T> * std::pair<K,T> reduceByKey( IreduceByKeyIFunctionP<K,T> funcP ){
				//return reduceByKey((void*) funcP, OP_Reduce);
			//}
			std::pair<K,T> bulkReduce( IbulkReduceIFunctionP<K,T> funcP ){
				return reduce((void*) funcP, OP_BulkReduce);
			}
			
			// --------------- FDD Builtin functions ------------- // 
			// Collect a FDD
			std::vector<std::pair<K,T>> collect( ){
				//std::cerr << " \033[0;31mSIZE: " << this->size << "\033[0m";
				std::vector<std::pair<K,T>> data(this->size);
				this->context->collectFDD(data, this);
				return data;
			}

			indexedFdd<K,T> * cache(){
				this->cached = true;
				return this;
			}
	};

	template <typename K, typename T> 
	class indexedFdd<K,T *> : public iFddCore<K,T*>{
		private:
			std::tuple <K,T,size_t>  finishReducesP(char ** partResult, size_t * pSize, int funcId, fddOpType op);
			std::tuple <K,T,size_t> reduceP( void * funcP, fddOpType op);

		public:
			// -------------- Constructors --------------- //
			//
			// Create a empty fdd
			indexedFdd(fastContext &c) : iFddCore<K,T *>(c){ 
				this->_tType = POINTER | decodeType(typeid(T).hash_code());
				this->id = c.createIPFDD(this, typeid(K).hash_code(), typeid(T).hash_code());
			}

			// Create a empty fdd with a pre allocated size
			indexedFdd(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) : iFddCore<K,T *>(c, s, dataAlloc){ 
				this->_tType = POINTER | decodeType(typeid(T).hash_code());
				this->id = c.createIPFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), c.getAllocation(s));
			}
			indexedFdd(fastContext &c, size_t s) : indexedFdd(c, s, c.getAllocation(s)) {
			}

			// Create a fdd from a array in memory
			indexedFdd(fastContext &c, K * keys, T ** data, size_t * dataSizes, size_t size) : indexedFdd(c, size){
				c.parallelizeI(this->id, keys, data, dataSizes, size);
			}

			~indexedFdd(){
			}


			// -------------- FDD Functions Parameter Specification --------------- //
			// These need to be specialized because they can return a pointer or not 

			// -------------------- Map ------------------- //

			// Map
			template <typename L, typename U> 
			indexedFdd<L,U> * map( ImapIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( IPmapIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			fdd<U> * map( mapIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			fdd<U> * map( PmapIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_Map);
			}
		
			// MapByKey
			template <typename L, typename U> 
			indexedFdd<L,U> * mapByKey( ImapByKeyIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * mapByKey( IPmapByKeyIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			fdd<U> * mapByKey( mapByKeyIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			fdd<U> * mapByKey( PmapByKeyIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_MapByKey);
			}


			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IbulkMapIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IPbulkMapIPFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkMap( bulkMapIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkMap( PbulkMapIPFunctionP<K,T,U> funcP ){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkMap);
			}


			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IflatMapIPFunctionP<K,T,L,U> funcP){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IPflatMapIPFunctionP<K,T,L,U> funcP){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * flatMap( flatMapIPFunctionP<K,T,U> funcP){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * flatMap( PflatMapIPFunctionP<K,T,U> funcP){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_FlatMap);
			}


			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapIPFunctionP<K,T,L,U> funcP){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapIPFunctionP<K,T,L,U> funcP){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkFlatMap( bulkFlatMapIPFunctionP<K,T,U> funcP){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			fdd<U> * bulkFlatMap( PbulkFlatMapIPFunctionP<K,T,U> funcP){
				return iFddCore<K,T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			
			// ------------------ Reduce ----------------- //

			// Run a Reduce
			inline std::vector<std::pair<K,T>> reduce(IPreduceIPFunctionP<K,T> funcP  ){
				return reduce((void*) funcP, OP_Reduce);
			}
			//inline indexedFdd<K,T> reduceByKey(IPreduceByKeyIPFunctionP<K,T> funcP  ){
				//return reduceByKey((void*) funcP, OP_Reduce);
			//}
			inline std::vector<std::pair<K,T>> bulkReduce(IPbulkReduceIPFunctionP<K,T> funcP  ){
				return reduce((void*) funcP, OP_BulkReduce);
			}
			
			// --------------- FDD Builtin functions ------------- // 
			// Collect a FDD
			std::vector<std::tuple<K,T*, size_t>> collect( ) {
				std::vector<std::tuple<K,T*, size_t>> data(this->size);
				this->context->collectFDD(data, this);
				return data;
			}
			
			indexedFdd<K,T*> * cache(){
				this->cached = true;
				return this;
			}

	};


	template <typename K, typename T> 
	void iFddCore<K,T>::update( void * funcP, fddOpType op){
		//std::cerr << "  Map ";
		unsigned long int tid, sid;
		auto start = system_clock::now();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, 0, funcId, this->size);

		// Receive results
		auto result = context->recvTaskResult(tid, sid, start);

		if (!cached)
			this->discard();

		//std::cerr << "\n";
	}

	template <typename K, typename T> 
	fddBase * iFddCore<K,T>::_map( void * funcP, fddOpType op, fddBase * newFdd, system_clock::time_point & start){
		//std::cerr << "  Map ";
		unsigned long int tid, sid;
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		auto result = context->recvTaskResult(tid, sid, start);

		if ( (op & 0xff) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ) {
			size_t newSize = 0;

			for (int i = 1; i < context->numProcs(); ++i){
				if (result[i].second > 0) newSize += * (size_t*) result[i].first;
			}

			newFdd->setSize(newSize);
		}

		if (!cached)
			this->discard();

		//std::cerr << "\n";
		return newFdd;
	}
	template <typename K, typename T> 
	template <typename L, typename U> 
	indexedFdd<L,U> * iFddCore<K,T>::mapI( void * funcP, fddOpType op){
		indexedFdd<L,U> * newFdd;
		auto start = system_clock::now();
		
		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<L,U>(*context);
		}else{
			if (dataAlloc.empty()) dataAlloc = context->getAllocation(size);
			newFdd = new indexedFdd<L,U>(*context, size, dataAlloc);
		}
		
		return (indexedFdd<L,U> *) _map(funcP, op, newFdd, start);
	}

	template <typename K, typename T> 
	template <typename U> 
	fdd<U> * iFddCore<K,T>::map( void * funcP, fddOpType op){
		fdd<U> * newFdd;
		auto start = system_clock::now();

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new fdd<U>(*context);
		}else{
			if (dataAlloc.empty()) dataAlloc = context->getAllocation(size);
			newFdd = new fdd<U>(*context, size, dataAlloc);
		}
		
		return (fdd<U> *) _map(funcP, op, newFdd, start);
	}

	template <typename K, typename T> 
	std::unordered_map<K,size_t> iFddCore<K,T>::countByKey(){
		//std::cerr << "  Count By Key";
		fastCommBuffer decoder(0);
		unsigned long int tid, sid;
		std::unordered_map<K,size_t> count;

		auto start = system_clock::now();
		context->enqueueTask(OP_CountByKey, id, this->size);

		auto result = context->recvTaskResult(tid, sid, start);

		for (int i = 1; i < context->numProcs(); ++i){

			K key;
			size_t kCount, numKeys;
			decoder.setBuffer(result[i].first, result[i].second);
			decoder >> numKeys;

			for ( size_t i = 0; i < numKeys; ++i ) {
				decoder >> key >> kCount;
				//auto it = count.find(key);
				//if (it != count.end())
				//	count[key] += kCount;
				//else
				//	count[key] = kCount;
				count[key] += kCount;
			}
		}

		//std::cerr << "\n";
		return count;
	}



	template <typename K, typename T> 
	std::unordered_map<K, std::tuple<size_t, int, size_t>> * iFddCore<K,T>::calculateKeyCount(std::vector< std::pair<void *, size_t> > & result){ 
		fastCommBuffer decoder(0);

		auto * count = new std::unordered_map< K, std::tuple<size_t, int, size_t> >();
		count->reserve(this->size);

		for (int i = 1; i < context->numProcs(); ++i){
			K key;
			size_t kCount, numKeys;

			if (result[i].second == 0) continue;

			decoder.setBuffer(result[i].first, result[i].second);
			decoder >> numKeys;

			for ( size_t j = 0; j < numKeys; ++j ) {

				decoder >> key >> kCount;
				auto it = count->find(key);

				if (it != count->end()){

					int &owner = std::get<1>(it->second);
					size_t &ownerCount = std::get<2>(it->second);

					std::get<0>(it->second) += kCount;

					// Fount the new majority owner
					if (kCount > ownerCount){
						owner = i;
						ownerCount = kCount;
					}

				}else{
					(*count)[key] = std::make_tuple(kCount, i, kCount);
				}
			}
		}

		return count;
	}
	template <typename K, typename T> 
		std::unordered_map<K, int> iFddCore<K,T>::calculateKeyMap(std::unordered_map<K, std::tuple<size_t, int, size_t>> & count){ 
			size_t size = this->size;
		std::unordered_map<K, int> kMap(count.size());
		std::unordered_map<K, bool> done;
		size_t numProcs = context->numProcs();
		std::vector<size_t> keyAlloc(numProcs,0);
		std::vector<size_t> procBudget = context->getAllocation(size);


		//std::cerr << "      [ Budget: ";
		//for ( int i = 1; i < numProcs; ++i)
			//std::cerr << procBudget[i] << " ";
		//std::cerr << "= " << size << "\n";

		for (auto it = count.begin(); it != count.end(); it++){
			K key = it->first;
			size_t kCount = std::get<0>(it->second);
			int preffered = std::get<1>(it->second);

			if(keyAlloc[preffered] < procBudget[preffered]){
				kMap[key] = preffered;
				keyAlloc [preffered] += kCount;
				//count.erase(key);
				done[key] = true;
			}else{
				done[key] = false;
			}
			
		}

		for (auto it = count.begin(); it != count.end(); it++){
			K key = it->first;
			if (! done[key]){
				size_t kCount = std::get<0>(it->second);
				int preffered = 1 + rand() % (numProcs - 1);

				while(keyAlloc[preffered] >= (procBudget[preffered] + 1)){
					//preffered = 1 + rand() % (numProcs - 1);
					preffered = (preffered + 1) % numProcs;
				}
				kMap[key] = preffered;
				keyAlloc [preffered] += kCount;
			}
		}
		/*std::cerr << "      [ Alloc: ";
		for ( int i = 1; i < numProcs; ++i)
			std::cerr << keyAlloc[i] << " ";
		std::cerr << "\n";
		std::cerr << "      [ Map: ";
		for (auto it = kMap.begin(); it != kMap.end(); it++){
		      std::cerr << it->first << ":" << it->second << "  ";
		}
		std::cerr << " ]\n"; */ 

		
		return kMap;
	}

	template <typename K, typename T> 
	indexedFdd<K,T> * iFddCore<K,T>::groupByKeyMapped(){
		unsigned long int tid, sid;

		if (! groupedByKey){
			using std::chrono::system_clock;
			using std::chrono::duration_cast;
			using std::chrono::milliseconds;
			std::cerr << "  GroupByKey ";
			auto start = system_clock::now();

			context->enqueueTask(OP_CountByKey, id, this->size);

			auto result = context->recvTaskResult(tid, sid, start);
			std::cerr << " CBK:" << duration_cast<milliseconds>(system_clock::now() - start).count();
			start = system_clock::now();

			// Get a count by key with majority owner consideration
			auto * count = calculateKeyCount(result);
			std::cerr << " proc.Keys:" << duration_cast<milliseconds>(system_clock::now() - start).count();
			auto start2 = system_clock::now();

			std::unordered_map<K, int> keyMap = calculateKeyMap(*count);
			delete count;
			std::cerr << " calc.KeyMap:" << duration_cast<milliseconds>(system_clock::now() - start2).count();
			start2 = system_clock::now();

			// Migrate data according to key ownership
			unsigned long int tid = context->enqueueTask(OP_GroupByKey, id, this->size);
			std::cerr << " enq.Task:" << duration_cast<milliseconds>(system_clock::now() - start2).count();
			start2 = system_clock::now();
			context->sendKeyMap(tid, keyMap);
			keyMap.clear();
			std::cerr << " snd.KeyMap:" << duration_cast<milliseconds>(system_clock::now() - start2).count();

			dataAlloc.resize(context->numProcs());
			result = context->recvTaskResult(tid, sid, start);
			size_t newSize = 0;
			for (int i = 1; i < context->numProcs(); ++i){
				if (result[i].second > 0){
					dataAlloc[i] = * (size_t*) result[i].first; 
					newSize += dataAlloc[i];
				}
			}
			size = newSize;
			groupedByKey = true;
		}
		//std::cerr << ". ";
		return (indexedFdd<K,T> *)this;
	}
	template <typename K, typename T> 
	indexedFdd<K,T> * iFddCore<K,T>::groupByKeyHashed(){
		unsigned long int tid, sid;

		if (! groupedByKey){
			auto start = system_clock::now();
			//std::cerr << "  GroupByKeyHashed ";

		
			// Migrate data according to key ownership
			tid = context->enqueueTask(OP_GroupByKeyH, id, this->size);

			auto result = context->recvTaskResult(tid, sid, start);
			size_t newSize = 0;
			dataAlloc.resize(context->numProcs());
			for (int i = 1; i < context->numProcs(); ++i){
				if (result[i].second > 0){
					dataAlloc[i] = * (size_t*) result[i].first; 
					newSize += dataAlloc[i];
				}
			}
			size = newSize;
			groupedByKey = true;
		}
		//std::cerr << ". ";

		return (indexedFdd<K,T> *)this;
	}
	template <typename K, typename T> 
	indexedFdd<K,T> * iFddCore<K,T>::groupByKey(){
		if (groupedByMap)
			return groupByKeyMapped();
		else
			return groupByKeyHashed();
	}

	template <typename K, typename T> 
	void iFddCore<K,T>::writeToFile(std::string path, std::string sufix){
		context->writeToFile(id, path, sufix);
	}


	template <typename K, typename T> 
	std::pair <K,T>  indexedFdd<K,T>::finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op){
		std::pair <K,T> result;

		if (op == OP_Reduce){
			IreduceIFunctionP<K, T> reduceFunc = (IreduceIFunctionP<K, T>) this->context->funcTable[funcId];
			fastCommBuffer buffer(0);

			// Get the real object behind the buffer
			buffer.setBuffer(partResult[0], pSize[0]);
			buffer >> result;

			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				std::pair <K,T> pr;

				//std::cerr << "   BUFFER: " << size_t(buffer.pos()) << " " << buffer.size() << "\n";
				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				result = reduceFunc(result.first, result.second, pr.first, pr.second);
			}
		}else{
			IbulkReduceIFunctionP<K, T> bulkReduceFunc = (IbulkReduceIFunctionP<K, T>) this->context->funcTable[funcId];
			T * vals = new T[this->context->numProcs() - 1];
			K * keys = new K[this->context->numProcs() - 1];

			//#pragma omp parallel for
			for (int i = 0; i < (this->context->numProcs() - 1); ++i){
				fastCommBuffer buffer(0);
				std::pair <K,T> pr;

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				keys[i] = pr.first;
				vals[i] = pr.second;
			}

			result = bulkReduceFunc(keys, vals, this->context->numProcs() - 1);

			delete [] vals;
			delete [] keys;
			// TODO do bulkreduce	
		}

		return result;
	}

	template <typename K, typename T> 
	std::pair <K,T> indexedFdd<K,T>::reduce( void * funcP, fddOpType op){
		//std::cerr << "  Reduce \n";
		std::pair <K,T> result;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char*[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];
		unsigned long int tid, sid;

		// Send task
		auto start = system_clock::now();
		unsigned long int reduceTaskId UNUSED = this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		auto resultV = this->context->recvTaskResult(tid, sid, start);

		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			partResult[i] = (char*) resultV[i + 1].first;
			rSize[i] = resultV[i + 1].second;
		}

		// Finish applying reduces
		result = finishReduces(partResult, rSize, funcId, op);

		delete [] partResult;
		delete [] rSize;

		if (!this->cached)
			this->discard();


		//std::cerr << "\n";
		return result;
	}


	template <typename K, typename T> 
	std::tuple <K,T,size_t>  indexedFdd<K,T*>::finishReducesP(char ** partResult, size_t * pSize, int funcId, fddOpType op){
		std::tuple <K,T,size_t> result;

		if (op == OP_Reduce){
			IPreduceIPFunctionP<K,T> reduceFunc = (IreduceIFunctionP<K,T>) this->context->funcTable[funcId];
			fastCommBuffer buffer(0);

			buffer.setBuffer(partResult[0], pSize[0]);
			buffer >> result;

			//#pragma omp parallel for
			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				std::tuple <K,T,size_t> pr;

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				result = reduceFunc(
						std::get<0>(result), 
						std::get<1>(result), 
						std::get<2>(result), 
						std::get<0>(pr), 
						std::get<1>(pr), 
						std::get<2>(pr));
			}
		}else{
			IPbulkReduceIPFunctionP<K,T> bulkReduceFunc = (IPbulkReduceIPFunctionP<K,T>) this->context->funcTable[funcId];
			T * vals = new T[this->context->numProcs() - 1];
			K * keys = new K[this->context->numProcs() - 1];
			size_t * sizes = new size_t[this->context->numProcs() - 1];

			//#pragma omp parallel for
			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				fastCommBuffer buffer(0);
				std::tuple <K,T,size_t> pr;

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				std::tie (keys[i], vals[i], sizes[i]) = pr;
			}

			result = bulkReduceFunc(keys, vals, sizes, this->context->numProcs() - 1);
			// TODO do bulkreduce	
		}

		return result;
	}

	template <typename K, typename T> 
	std::tuple <K,T,size_t> indexedFdd<K,T*>::reduceP( void * funcP, fddOpType op){
		auto start = system_clock::now();
		//std::cerr << "  Reduce ";
		std::tuple <K,T,size_t> result;
		unsigned long int tid, sid;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char *[this->context->numProcs() - 1];
		size_t * partrSize = new size_t[this->context->numProcs() - 1];

		// Send task
		unsigned long int reduceTaskId = this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		auto resultV = this->context->recvTaskResult(tid, sid, start);

		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			partResult[i] = (T*) resultV[i+1].first;
			partrSize[i] = resultV[i+1].second /= sizeof(T);
		}

		// Finish applying reduces
		result = finishReducesP(partResult, partrSize, funcId, op);

		delete [] partResult;
		delete [] partrSize;

		if (!this->cached)
			this->discard();

		//std::cerr << "\n";
		return result;
	}	

}

#endif
