#ifndef LIBFASTER_PAIDFDD_H
#define LIBFASTER_PAIDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

#include "fddBase.h"
#include "fastContext.h"
#include "fddStorage.h"

template <class K, class T> 
class indexedFdd ; 

// Driver side FDD
// It just sends commands to the workers.
template <class K, class T> 
class iFddCore : public fddBase{
	private:
		fastContext * context;

	public:
		iFddCore() {}
		
		// Create a empty fdd
		template <typename U>
		iFddCore(U &c) {
			context = &c;
			id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		iFddCore(U &c, size_t s) {
			context = &c;
			size = s;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
		}

		~iFddCore(){}

		// -------------- Core FDD Functions --------------- //
		template <typename L, typename U> 
		indexedFdd<L,U> * map( void * funcP, fddOpType op){
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
		template <typename U>
		indexedFdd(U &c) : iFddCore<K,T>(c){ }

		// Create a empty fdd with a pre allocated size
		template <typename U>
		indexedFdd(U &c, size_t s) : iFddCore<K,T>(c, s){ }

		// Create a fdd from a array in memory
		template <typename U>
		indexedFdd(U &c, K * keys, T * data, size_t size) : indexedFdd(c, size){
			c.parallelize(fddBase::id, data, size);
		}

		/* // Create a fdd from a file
		template <typename L, typename U>
		indexedFdd(U &c, const char * fileName) {
			context = &c;
			id = c.readFDD(this, fileName);

			// Recover FDD information (size, ? etc )
			context->getFDDInfo(size);
		}// */

		~indexedFdd(){
		}

		// -------------- FDD Functions --------------- //

 		// -------------------- Map ------------------- //
		// Map
		template <typename L, typename U> 
		indexedFdd<L,U> * map( ImapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( IPmapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		fdd<U> * map( mapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		fdd<U> * map( PmapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, Map);
		}


		// BulkMap
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IbulkMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IPbulkMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkMap( bulkMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkMap( PbulkMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkMap);
		}


		// FlatMap
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IflatMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IPflatMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * flatMap( flatMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * flatMap( PflatMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, FlatMap);
		}


		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapIFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapIFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkFlatMap);
		}

 		// ------------------ Reduce ----------------- //

		// Run a Reduce
		std::pair<K,T> reduce( IreduceIFunctionP<K,T> funcP ){
			return iFddCore<K,T>::reduce((void*) funcP, Reduce);
		}
		std::pair<K,T> bulkReduce( IbulkReduceIFunctionP<K,T> funcP ){
			return iFddCore<K,T>::reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> * collect( ){
			return iFddCore<K,T>::context->collectRDD(this);
		}
		void * _collect( ) override{
			return iFddCore<K,T>::context->collectRDD(this);
		}

};

template <class K, class T> 
class indexedFdd<K,T *> : public fddBase{
	public:
		// -------------- Constructors --------------- //
		//
		// Create a empty fdd
		template <typename U>
		indexedFdd(U &c) : iFddCore<K,T *>(c){ }

		// Create a empty fdd with a pre allocated size
		template <typename U>
		indexedFdd(U &c, size_t s) : iFddCore<K,T *>(c, s){ }

		// Create a fdd from a array in memory
		template <typename U>
		indexedFdd(U &c, T ** data, size_t * dataSizes, size_t size) : indexedFdd(c, size){
			c.parallelize(id, data, dataSizes, size);
		}

		~indexedFdd(){
		}


		// -------------- FDD Functions Parameter Specification --------------- //
		// These need to be specialized because they can return a pointer or not 

 		// -------------------- Map ------------------- //

		// Map
		template <typename L, typename U> 
		indexedFdd<L,U> * map( ImapIPFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( IPmapIPFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		fdd<U> * map( mapIPFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, Map);
		}
		template <typename L, typename U> 
		fdd<U> * map( PmapIPFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, Map);
		}


		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IbulkMapIPFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IPbulkMapIPFunctionP<K,T,L,U> funcP ){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkMap( bulkMapIPFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkMap( PbulkMapIPFunctionP<K,T,U> funcP ){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkMap);
		}


		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IflatMapIPFunctionP<K,T,L,U> funcP){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IPflatMapIPFunctionP<K,T,L,U> funcP){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * flatMap( flatMapIPFunctionP<K,T,U> funcP){
			return iFddCore<K,T>::template map<U>((void*) funcP, FlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * flatMap( PflatMapIPFunctionP<K,T,U> funcP){
			return iFddCore<K,T>::template map<U>((void*) funcP, FlatMap);
		}


		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapIPFunctionP<K,T,L,U> funcP){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapIPFunctionP<K,T,L,U> funcP){
			return iFddCore<K,T>::template map<L,U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapIPFunctionP<K,T,U> funcP){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename L, typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapIPFunctionP<K,T,U> funcP){
			return iFddCore<K,T>::template map<U>((void*) funcP, BulkFlatMap);
		}
		
 		// ------------------ Reduce ----------------- //

		// Run a Reduce
		inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
			return iFddCore<K,T>::reduce((void*) funcP, Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return iFddCore<K,T>::reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::map<K,T*> * collect( ) {
			return iFddCore<K,T>::context->collectRDD(this);
		}
		void * _collect( ) override{
			return iFddCore<K,T>::context->collectRDD(this);
		}

};

#endif
