#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

class fastTask;

template <class K, class T> 
class indexedFdd ; 


#include "definitions.h"
//#include "fastTask.h" 
#include "fddBase.h"
#include "fastContext.h"
//#include "indexedFdd.h"

template <typename T> 
class fddCore : public fddBase {
	protected :
		fastContext * context;

		// -------------- Core FDD Functions --------------- //

		fddCore() {}

		fddCore(fastContext & c);

		// Create a empty fdd with a pre allocated size
		fddCore(fastContext &c, size_t s);

		// 1->1 function (map, bulkmap, flatmap...)
		template <typename U> 
		fdd<U> * map( void * funcP, fddOpType op);

};

// Driver side FDD
// It just sends commands to the workers.
template <class T> 
class fdd : public fddCore<T>{
	private:
		T finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op);
		T reduce( void * funcP, fddOpType op);
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		fdd(fastContext &c) : fddCore<T>(c){ 
			this->id = c.createFDD(this,  typeid(T).hash_code());
		}

		// Create a empty fdd with a pre allocated size
		fdd(fastContext &c, size_t s) : fddCore<T>(c, s){ 
			this->id = c.createFDD(this,  typeid(T).hash_code(), s);
		}

		// Create a fdd from a array in memory
		fdd(fastContext &c, T * data, size_t size) : fdd(c, size){
			c.parallelize(fddBase::id, data, size);
		}

		// Create a fdd from a vector in memory
		fdd(fastContext &c, std::vector<T> &dataV) : fdd(c, dataV.data(), dataV.size()){ }

		// Create a fdd from a file
		fdd(fastContext &c, const char * fileName) ;

		~fdd(){
		}

		// -------------- FDD Functions --------------- //

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_Map);
		}
		template <typename U> 
		fdd<U> * map( PmapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( ImapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( IPmapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IbulkMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IPbulkMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IflatMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IPflatMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
		}


		// Run a Reduce
		T reduce( reduceFunctionP<T> funcP ){
			return reduce((void*) funcP, OP_Reduce);
		}
		T bulkReduce( bulkReduceFunctionP<T> funcP ){
			return reduce((void*) funcP, OP_BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> collect( ){
			std::vector<T> data(this->size);
			this->context->collectFDD(data, this);
			return data;
		}
		/*void * _collect( ) override{
			return fddCore<T>::context->collectFDD(this);
		}*/

};

template <class T> 
class fdd<T *> : public fddCore<T>{
	private:
		std::vector <T> finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op);
		std::vector <T> reduceP(void * funcP, fddOpType op);
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		fdd(fastContext &c) : fddCore<T>(c){ 
			this->id = c.createPFDD(this,  typeid(T).hash_code());
		}

		// Create a empty fdd with a pre allocated size
		fdd(fastContext &c, size_t s) : fddCore<T>(c, s){ 
			this->id = c.createPFDD(this,  typeid(T).hash_code(), s);
		}

		// Create a fdd from a array in memory
		fdd(fastContext &c, T * data[], size_t dataSizes[], size_t size) : fdd(c, size){
			c.parallelize(fddBase::id, data, dataSizes, size);
		}

		~fdd(){
		}


		// -------------- FDD Functions Parameter Specification --------------- //
		// These need to be specialized because they can return a pointer or not 

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_Map);
		}
		template <typename U> 
		fdd<U> * map( PmapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( ImapPFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_Map);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * map( IPmapPFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IbulkMapPFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkMap( IPbulkMapPFunctionP<T,L,U> funcP ){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IflatMapPFunctionP<T,L,U> funcP){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_FlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * flatMap( IPflatMapPFunctionP<T,L,U> funcP){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapPFunctionP<T,L,U> funcP){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
		}
		template <typename L, typename U> 
		indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapPFunctionP<T,L,U> funcP){
			return fddCore<T>::template map<L,U>((void*) funcP, OP_BulkFlatMap);
		}

		// Run a Reduce
		inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
			return reduceP((void*) funcP, OP_Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return reduceP((void*) funcP, OP_BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<std::pair<T*, size_t>> collect( ) {
			std::vector<std::pair<T*, size_t>> data(this->size);
			this->context->collectFDD(data, this);
			return data;
		}
		/*void * _collect( ) override{
			return fddCore<T>::context->collectFDD(this);
		}*/

};

template <typename T> 
template <typename U> 
fdd<U> * fddCore<T>::map( void * funcP, fddOpType op){
	fdd<U> * newFdd ;
	size_t result;
	size_t rSize;

	if ( (op & 0xFF ) & (OP_FlatMap | OP_BulkFlatMap) ){
		newFdd = new fdd<U>(*context);
	}else{
		newFdd = new fdd<U>(*context, size);
	}
	unsigned long int newFddId = newFdd->getId();

	// Decode function pointer
	int funcId = context->findFunc(funcP);
	//std::cerr << " " << funcId << ".\n";

	// Send task
	context->enqueueTask(op, id, newFddId, funcId);

	// Receive results
	for (int i = 1; i < context->numProcs(); ++i){
		result = * (char*) context->recvTaskResult(id, rSize);
	}

	return newFdd;
}

#endif
