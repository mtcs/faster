#ifndef LIBFASTER_INDEXEDFDD_H
#define LIBFASTER_INDEXEDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

class fastContext;

#include "definitions.h"
#include "fddBase.h"
#include "fastContext.h"

template <class K, class T> 
class indexedFdd ; 

// Driver side FDD
// It just sends commands to the workers.
template <class K, class T> 
class iFddCore : public fddBase{
	private:
		fastContext * context;

		iFddCore() {}
		
		// Create a empty fdd
		iFddCore(fastContext &c) {
			context = &c;
		}

		// Create a empty fdd with a pre allocated size
		iFddCore(fastContext &c, size_t s) {
			context = &c;
			size = s;
		}

		~iFddCore(){}

		// -------------- Core FDD Functions --------------- //
		template <typename L, typename U> 
		indexedFdd<L,U> * map( void * funcP, fddOpType op){
			indexedFdd<L,U> * newFdd = new indexedFdd<L,U>(*context, size);
			unsigned long int newFddId = newFdd->getId();
			char result;
			size_t rSize;

			// Decode function pointer
			int funcId = context->findFunc(funcP);

			// Send task
			context->enqueueTask(op, id, newFddId, funcId);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				result = * (char*) context->recvTaskResult(id, rSize);
			}
		}

};

template <class K, class T> 
class indexedFdd : public iFddCore<K,T>{
	private:
		std::pair <K,T>  finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op){
			std::pair <K,T> result;

			if (op == OP_Reduce){
				reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) this->context->funcTable[funcId];
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
				bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) this->context->funcTable[funcId];
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

		std::pair <K,T> reduce( void * funcP, fddOpType op){
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



	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		indexedFdd(fastContext &c) : iFddCore<K,T>(c){ 
			this->id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code());
		}

		// Create a empty fdd with a pre allocated size
		indexedFdd(fastContext &c, size_t s) : iFddCore<K,T>(c, s){ 
			this->id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), s);
		}

		// Create a fdd from a array in memory
		indexedFdd(fastContext &c, K keys[], T data[], size_t size) : indexedFdd(c, size){
			c.parallelize(fddBase::id, data, size);
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
		std::vector<T> collect( ){
			return iFddCore<K,T>::context->collectRDD(this);
		}
		/*void * _collect( ) override{
			return iFddCore<K,T>::context->collectRDD(this);
		}*/

};

template <class K, class T> 
class indexedFdd<K,T *> : public fddBase{
	private:
		std::tuple <K,T,size_t>  finishReducesP(char ** partResult, size_t * pSize, int funcId, fddOpType op){
			std::tuple <K,T,size_t> result;

			if (op == OP_Reduce){
				reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) this->context->funcTable[funcId];
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
				bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) this->context->funcTable[funcId];
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

		std::tuple <K,T,size_t> reduceP( void * funcP, fddOpType op){
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
		}	public:
		// -------------- Constructors --------------- //
		//
		// Create a empty fdd
		indexedFdd(fastContext &c) : iFddCore<K,T *>(c){ 
			this->id = c.createIPFDD(this, typeid(K).hash_code(), typeid(T).hash_code());
		}

		// Create a empty fdd with a pre allocated size
		indexedFdd(fastContext &c, size_t s) : iFddCore<K,T *>(c, s){ 
			this->id = c.createIPFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), s);
		}

		// Create a fdd from a array in memory
		indexedFdd(fastContext &c, K keys[], T * data[], size_t dataSizes[], size_t size) : indexedFdd(c, size){
			c.parallelize(id, keys, data, dataSizes, size);
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
		std::map<K,T*> collect( ) {
			return iFddCore<K,T>::context->collectRDD(this);
		}
		/*void * _collect( ) override{
			return iFddCore<K,T>::context->collectRDD(this);
		}*/

};

#endif
