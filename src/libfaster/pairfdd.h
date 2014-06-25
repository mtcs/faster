#ifndef LIBFASTER_PAIDFDD_H
#define LIBFASTER_PAIDFDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

#include "fddBase.h"
#include "fastContext.h"
#include "fddStorage.h"
#include "misc.h"

// Driver side FDD
// It just sends commands to the workers.
template <class K, class T> 
class indexedFdd : public fddBase{
	private:
		fastContext * context;

		// -------------- Core FDD Functions --------------- //
		template <typename U> 
		indexedFdd<K,U> * _map( void * funcP, fddOpType op){
		}

		std::map <K,T> _reduce( void * funcP, fddOpType op){
		}

	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		indexedFdd(U &c) {
			context = &c;
			id = c.createIFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		indexedFdd(U &c, size_t s) {
			context = &c;
			size = s;
			id = c.createIFDD(this,  typeid(T).hash_code(), size);
		}

		// Create a fdd from a array in memory
		template <typename U>
		indexedFdd(U &c, T * data, size_t size) : indexedFdd(c, size){
			c.parallelize(id, data, size);
		}

		// Create a fdd from a vector in memory
		template <typename U>
		indexedFdd(U &c, std::vector<T> &data) : indexedFdd(c, data.data(),data.size()){ }

		// Create a fdd from a file
		template <typename U>
		indexedFdd(U &c, const char * fileName) {
			context = &c;
			id = c.readFDD(this, fileName);

			// Recover FDD information (size, ? etc )
			context->getFDDInfo(size);
		}

		~indexedFdd(){
		}

		// -------------- FDD Functions --------------- //

		// Run a Map
		template <typename U> 
		indexedFdd<K,U> * map( ImapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}
		template <typename U> 
		indexedFdd<K,U> * map( IPmapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( mapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( PmapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}


		template <typename U> 
		indexedFdd<K,U> * bulkMap( IbulkMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		indexedFdd<K,U> * bulkMap( IPbulkMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		indexedFdd<K,U> * flatMap( IflatMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		indexedFdd<K,U> * flatMap( IPflatMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		indexedFdd<K,U> * bulkFlatMap( IbulkFlatMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		indexedFdd<K,U> * bulkFlatMap( IPbulkFlatMapIFunctionP<K,T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkFlatMap);
		}

		// TODO if it returns a pointer to U, specialize...

		// Run a Reduce
		std::pair<K,T> reduce( IreduceIFunctionP<K,T> funcP ){
			return _reduce((void*) funcP, Reduce);
		}
		std::pair<K,T> bulkReduce( IbulkReduceIFunctionP<K,T> funcP ){
			return _reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> * collect( ){
			return context->collectRDD<T>(id);
		}
		void * _collect( ) override{
			return context->collectRDD<T>(id);
		}

};

template <class K, class T> 
class indexedFdd<K,T *> : public fddBase{
	private:
		fastContext * context;

		// -------------- Core FDD Functions --------------- //
		template <typename U> 
		indexedFdd<K,U> * _map( void * funcP, fddOpType op){
		}

		std::map <K,T> _reduce( void * funcP, fddOpType op){
		}


	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		indexedFdd(U &c) {
			context = &c;
			id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		indexedFdd(U &c, size_t s) {
			context = &c;
			size = s;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
		}

		// Create a fdd from a array in memory
		template <typename U>
		indexedFdd(U &c, T ** data, size_t * dataSizes, size_t size) : indexedFdd(c, size){
			c.parallelize(id, data, dataSizes, size);
		}

		~indexedFdd(){
		}


		// -------------- FDD Functions Parameter Specification --------------- //
		// These need to be specialized because they can return a pointer or not 

		// Run a Map
		template <typename U> 
		indexedFdd<K,U> * map( mapPFunctionP<T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}
		template <typename U> 
		indexedFdd<K,U> * map( PmapPFunctionP<T,U> funcP ){
			return _map<K,U>((void*) funcP, Map);
		}


		template <typename U> 
		indexedFdd<K,U> * bulkMap( bulkMapPFunctionP<T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		indexedFdd<K,U> * bulkMap( PbulkMapPFunctionP<T,U> funcP ){
			return _map<K,U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		indexedFdd<K,U> * flatMap( flatMapPFunctionP<T,U> funcP){
			return _map<K,U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		indexedFdd<K,U> * flatMap( PflatMapPFunctionP<T,U> funcP){
			return _map<K,U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		indexedFdd<K,U> * bulkFlatMap( bulkFlatMapPFunctionP<T,U> funcP){
			return _map<K,U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		indexedFdd<K,U> * bulkFlatMap( PbulkFlatMapPFunctionP<T,U> funcP){
			return _map<K,U>((void*) funcP, BulkFlatMap);
		}

		// Run a Reduce
		inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
			return _reduce((void*) funcP, Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return _reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T*> * collect( ) {
			return context->collectRDD<T>(id);
		}
		void * _collect( ) override{
			return context->collectRDD<T>(id);
		}

};

#endif
