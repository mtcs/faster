#ifndef LIBFASTER_INDEXEDFDD_H
#define LIBFASTER_INDEXEDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

class fastContext;

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
			id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		iFddCore(fastContext &c, size_t s) {
			context = &c;
			size = s;
			id = c.createIFDD(this, typeid(K).hash_code(), typeid(T).hash_code(), size);
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
				context->recvTaskResult(id, &result, rSize);
			}
		}

		std::map <K,T> reduce( void * funcP, fddOpType op){
		}
		std::map <K,T> reduceP( void * funcP, fddOpType op){
		}
};

template <class K, class T> 
class indexedFdd : public iFddCore<K,T>{

	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		indexedFdd(fastContext &c) : iFddCore<K,T>(c){ }

		// Create a empty fdd with a pre allocated size
		indexedFdd(fastContext &c, size_t s) : iFddCore<K,T>(c, s){ }

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
			return iFddCore<K,T>::reduce((void*) funcP, OP_Reduce);
		}
		std::pair<K,T> bulkReduce( IbulkReduceIFunctionP<K,T> funcP ){
			return iFddCore<K,T>::reduce((void*) funcP, OP_BulkReduce);
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
	public:
		// -------------- Constructors --------------- //
		//
		// Create a empty fdd
		indexedFdd(fastContext &c) : iFddCore<K,T *>(c){ }

		// Create a empty fdd with a pre allocated size
		indexedFdd(fastContext &c, size_t s) : iFddCore<K,T *>(c, s){ }

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
			return iFddCore<K,T>::reduce((void*) funcP, OP_Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return iFddCore<K,T>::reduce((void*) funcP, OP_BulkReduce);
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
