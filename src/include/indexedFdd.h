#ifndef LIBFASTER_INDEXEDFDD_H
#define LIBFASTER_INDEXEDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>


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
			inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
				return reduce((void*) funcP, OP_Reduce);
			}
			inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
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
		indexedFdd<L,U> * newFdd;
		char result;
		size_t rSize;

		if ( (op & 0xFF ) & (OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<L,U>(*context);
		}else{
			newFdd = new indexedFdd<L,U>(*context, size);
		}
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId);

		// Receive results
		for (int i = 1; i < context->numProcs(); ++i){
			result = * (char*) context->recvTaskResult(id, rSize);
		}

		return newFdd;
	}

	template <typename K, typename T> 
	CountKeyMapT<K> iFddCore<K,T>::countByKey(){
		context->enqueueTask(OP_CountByKey, id);

		return context->recvCountByKey<K>(this->size);
	}

	template <typename K, typename T> 
	indexedFdd<K,T> * iFddCore<K,T>::groupByKey(){
		char result;
		size_t rSize;

		if (! groupedByKey){
			context->enqueueTask(OP_GroupByKey, id);

			result = * (char*) context->recvTaskResult(id, rSize);
			groupedByKey = true;
		}
		return this;
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
		std::pair <K,T> result;
		unsigned long int tId;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char*[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];

		// Send task
		unsigned long int reduceTaskId = this->context->enqueueTask(op, this->id, 0, funcId);

		// Receive results
		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			char * pr = (char*) this->context->recvTaskResult(tId, rSize[i]);
			partResult[i] = new char [rSize[i]];
			memcpy(partResult[i], pr, rSize[i]);
		}

		// Finish applying reduces
		result = finishReduces(partResult, rSize, funcId, op);

		delete [] partResult;
		delete [] rSize;

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
		std::tuple <K,T,size_t> result;
		unsigned long int tId;
		int funcId = this->context->findFunc(funcP);
		char ** partResult = new char *[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];

		// Send task
		unsigned long int reduceTaskId = this->context->enqueueTask(op, this->id, 0, funcId);

		// Receive results
		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			char * pr = (char*) this->context->recvTaskResult(tId, rSize[i]);
			partResult[i] = new char [rSize[i]];
			memcpy(partResult[i], pr, rSize[i]);
		}

		// Finish applying reduces
		result = finishReducesP(partResult, rSize, funcId, op);

		delete [] partResult;
		delete [] rSize;

		return result;
	}	

}

#endif
