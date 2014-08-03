#ifndef LIBFASTER_INDEXEDFDD_H
#define LIBFASTER_INDEXEDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>
#include <tuple>


#include "definitions.h"
#include "fddBase.h"
#include "fastContext.h"
#include "misc.h"

namespace faster{

	class fastContext;

	template<typename... Types> 
	class groupedFdd;

	template <class K, class T> 
	class indexedFdd ; 

	// Driver side FDD
	// It just sends commands to the workers.
	template <class K, class T> 
	class iFddCore : public fddBase{

		protected:
			bool groupedByKey;
			fastContext * context;

			iFddCore() {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
			}
			
			// Create a empty fdd
			iFddCore(fastContext &c) {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
				context = &c;
			}

			// Create a empty fdd with a pre allocated size
			iFddCore(fastContext &c, size_t s) {
				_kType = decodeType(typeid(K).hash_code());
				groupedByKey = false;
				context = &c;
				this->size = s;
			}

			~iFddCore(){}

			std::unordered_map<T, int> calculateKeyMigration(std::unordered_map<T, std::tuple<size_t, int, size_t>> count);

			// -------------- Core FDD Functions --------------- //
			template <typename L, typename U> 
			indexedFdd<L,U> * map( void * funcP, fddOpType op);

		public:
			template<typename... FddTypes> 
			groupedFdd<iFddCore<K,T> *, FddTypes...> * cogroup(FddTypes &... fddTypes){
				return new groupedFdd<iFddCore<K,T>*, FddTypes...>(context, this, fddTypes...);
			}

			CountKeyMapT<K> countByKey();

			indexedFdd<K,T> * groupByKey();


	};

	template <class K, class T> 
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
			indexedFdd(fastContext &c, size_t s) : iFddCore<K,T>(c, s){ 
				this->_tType = decodeType(typeid(T).hash_code());
				this->id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), s);
			}

			// Create a fdd from a array in memory
			indexedFdd(fastContext &c, K * keys, T * data, size_t size) : indexedFdd(c, size){
				c.parallelizeI(this->id, keys, data, size);
			}

			~indexedFdd(){
			}

			// -------------- FDD Functions --------------- //

			// -------------------- Map ------------------- //
			// Map
			template <typename L, typename U> 
			indexedFdd<L,U> * map( ImapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( IPmapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_Map);
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
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_MapByKey);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * mapByKey( IPmapByKeyIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_MapByKey);
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
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IPbulkMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkMap);
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
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IPflatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_FlatMap);
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
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapIFunctionP<K,T,L,U> funcP ){
				return iFddCore<K,T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
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
				std::vector<std::pair<K,T>> data(this->size);
				this->context->collectFDD(data, this);
				return data;
			}
	};

	template <class K, class T> 
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
			indexedFdd(fastContext &c, size_t s) : iFddCore<K,T *>(c, s){ 
				this->_tType = POINTER | decodeType(typeid(T).hash_code());
				this->id = c.createIPFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), s);
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

	};


	template <typename K, typename T> 
	template <typename L, typename U> 
	indexedFdd<L,U> * iFddCore<K,T>::map( void * funcP, fddOpType op){
		std::cerr << "  Map\n";
		indexedFdd<L,U> * newFdd;
		size_t result;
		size_t rSize;
		unsigned long int tid, sid;

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<L,U>(*context);
		}else{
			newFdd = new indexedFdd<L,U>(*context, size);
		}
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		if ( (op & 0xff) & (OP_MapByKey | OP_FlatMap) )
			newFdd->size = 0;
		for (int i = 1; i < context->numProcs(); ++i){
			result = * (size_t*) context->recvTaskResult(tid, sid, rSize);
			if ( (op & 0xff) & (OP_MapByKey | OP_FlatMap) )
				newFdd->size += result;
		}

		std::cerr << "  Done\n";
		return newFdd;
	}

	template <typename K, typename T> 
	std::unordered_map<K,size_t> iFddCore<K,T>::countByKey(){
		std::cerr << "  Count By Key\n";
		fastCommBuffer decoder(0);
		void * result;
		size_t rSize;
		unsigned long int tid, sid;
		std::unordered_map<K,size_t> count;


		context->enqueueTask(OP_CountByKey, id, this->size);

		for (int i = 1; i < context->numProcs(); ++i){

			K key;
			size_t kCount, numKeys;
			result = context->recvTaskResult(tid, sid, rSize);
			decoder.setBuffer(result, rSize);
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

		std::cerr << "  Done\n";
		return count;
	}

	template <typename K, typename T> 
	std::unordered_map<T, int> iFddCore<K,T>::calculateKeyMigration(std::unordered_map<T, std::tuple<size_t, int, size_t>> count){ 
		size_t size = this->size;
		std::unordered_map<T, int> keyMap;
		size_t numProcs = context->numProcs();
		std::vector<size_t> keyAlloc(numProcs,0);
		std::vector<size_t> procBudget = context->getAllocation(size);

		keyMap.reserve(count.size());

		//std::cerr << "      [ Budget: ";
		//for ( int i = 1; i < numProcs; ++i)
			//std::cerr << procBudget[i] << " ";
		//std::cerr << "= " << size << "\n";

		for (auto it = count.begin(); it != count.end(); it++){
			T key = it->first;
			size_t kCount = std::get<0>(it->second);
			int preffered = std::get<1>(it->second);

			if(keyAlloc[preffered] < procBudget[preffered]){
				keyMap[key] = preffered;
				keyAlloc [preffered] += kCount;
				count.erase(key);
			}
		}

		for (auto it = count.begin(); it != count.end(); it++){
			T key = it->first;
			size_t kCount = std::get<0>(it->second);
			int preffered = 1 + rand() % (numProcs - 1);

			while(keyAlloc[preffered] >= procBudget[preffered]){
				preffered = 1 + rand() % (numProcs - 1);
			}
			keyMap[key] = preffered;
			keyAlloc [preffered] += kCount;
		}
		std::cerr << "      [ Alloc: ";
		for ( int i = 1; i < numProcs; ++i)
			std::cerr << keyAlloc[i] << " ";
		std::cerr << "\n";
		//std::cerr << "      [ Map: ";
		//for (auto it = keyMap.begin(); it != keyMap.end(); it++){
			//std::cerr << it->first << ":" << it->second << "  ";
		//}
		//std::cerr << "\n";

		
		return keyMap;
	}

	template <typename K, typename T> 
	indexedFdd<K,T> * iFddCore<K,T>::groupByKey(){
		std::cerr << "  Group By Key\n";
		fastCommBuffer decoder(0);
		void * result;
		size_t rSize;
		unsigned long int tid, sid;
		// Key -> totalKeycount, maxowner, ownerCount

		if (! groupedByKey){
			auto * count = new std::unordered_map<K, std::tuple<size_t, int, size_t>>(this->size);

			context->enqueueTask(OP_CountByKey, id, this->size);

			// Get a count by key with majority owner consideration
			for (int i = 1; i < context->numProcs(); ++i){
				K key;
				size_t kCount, numKeys;

				result = context->recvTaskResult(tid, sid, rSize);
				decoder.setBuffer(result, rSize);
				decoder >> numKeys;

				for ( size_t i = 0; i < numKeys; ++i ) {
					
					decoder >> key >> kCount;
					auto it = count->find(key);

					if (it != count->end()){

						int &owner = std::get<1>(it->second);
						size_t &ownerCount = std::get<2>(it->second);

						std::get<0>(it->second) += kCount;

						// Fount the new majority owner
						if (kCount > ownerCount){
							owner = sid;
							ownerCount = kCount;
						}

					}else{
						(*count)[key] = std::make_tuple(kCount, sid, kCount);
					}
				}
			}
			std::unordered_map<K, int> keyMap = calculateKeyMigration(*count);
			delete count;

			// Migrate data according to key ownership
			unsigned long int tid = context->enqueueTask(OP_GroupByKey, id, this->size);
			context->sendKeyMap(tid, keyMap);

			for (int i = 1; i < context->numProcs(); ++i){
				result = context->recvTaskResult(tid, sid, rSize);
			}
			groupedByKey = true;
		}
		std::cerr << "  Done\n";
		return (indexedFdd<K,T> *)this;
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

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				result = reduceFunc(result.first, result.second, pr.first, pr.second);
			}
		}else{
			IbulkReduceIFunctionP<K, T> bulkReduceFunc = (IbulkReduceIFunctionP<K, T>) this->context->funcTable[funcId];
			T * vals = new T[this->context->numProcs() - 1];
			K * keys = new K[this->context->numProcs() - 1];

			#pragma omp parallel for
			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				fastCommBuffer buffer(0);
				std::pair <K,T> pr;

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				keys[i] = pr.first;
				vals[i] = pr.second;
			}

			result = bulkReduceFunc(keys, vals, this->context->numProcs() - 1);
			// TODO do bulkreduce	
		}

		return result;
	}

	template <typename K, typename T> 
	std::pair <K,T> indexedFdd<K,T>::reduce( void * funcP, fddOpType op){
		std::cerr << "  Reduce\n";
		std::pair <K,T> result;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char*[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];
		unsigned long int tid, sid;

		// Send task
		unsigned long int reduceTaskId = this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			char * pr = (char*) this->context->recvTaskResult(tid, sid, rSize[i]);
			partResult[i] = new char [rSize[i]];
			memcpy(partResult[i], pr, rSize[i]);
		}

		// Finish applying reduces
		result = finishReduces(partResult, rSize, funcId, op);

		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			delete [] partResult[i];
		}
		delete [] partResult;
		delete [] rSize;

		std::cerr << "  Done\n";
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

			#pragma omp parallel for
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

			#pragma omp parallel for
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
		std::cerr << "  Reduce\n";
		std::tuple <K,T,size_t> result;
		unsigned long int tid, sid;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char *[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];

		// Send task
		unsigned long int reduceTaskId = this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			char * pr = (char*) this->context->recvTaskResult(tid, sid, rSize[i]);
			partResult[i] = new char [rSize[i]];
			memcpy(partResult[i], pr, rSize[i]);
		}

		// Finish applying reduces
		result = finishReducesP(partResult, rSize, funcId, op);

		delete [] partResult;
		delete [] rSize;

		std::cerr << "  Done\n";
		return result;
	}	

}

#endif
