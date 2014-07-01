#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

#include "fddBase.h"
#include "fastContext.h"
#include "indexedFdd.h"

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
		fdd<U> * map( void * funcP, fddOpType op){
			fdd<U> * newFdd ;
			if ( (op & 0xFF ) & (OP_FlatMap | OP_BulkFlatMap) )
				newFdd = new fdd<U>(*context);
			else
				newFdd = new fdd<U>(*context, size);
			unsigned long int newFddId = newFdd->getId();
			char result;
			size_t rSize;

			// Decode function pointer
			int funcId = context->findFunc(funcP);
			//std::cerr << " " << funcId << ".\n";

			// Send task
			context->enqueueTask(op, id, newFddId, funcId);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				context->recvTaskResult(id, &result, rSize);
			}

			return newFdd;
		}

		// TODO Function to return indexedFdd<L,U>

		T finishReduces(T * partResult, int funcId, fddOpType op);
		T reduce( void * funcP, fddOpType op);

		std::vector <T> finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op);
		std::vector <T> reduceP(void * funcP, fddOpType op);

};

// Driver side FDD
// It just sends commands to the workers.
template <class T> 
class fdd : public fddCore<T>{
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		fdd(fastContext &c) : fddCore<T>(c){ }

		// Create a empty fdd with a pre allocated size
		fdd(fastContext &c, size_t s) : fddCore<T>(c, s){ }

		// Create a fdd from a array in memory
		fdd(fastContext &c, T data[], size_t size) : fdd(c, size){
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

		// TODO if it returns a pointer to U, specialize...

		// Run a Reduce
		T reduce( reduceFunctionP<T> funcP ){
			return fddCore<T>::reduce((void*) funcP, OP_Reduce);
		}
		T bulkReduce( bulkReduceFunctionP<T> funcP ){
			return fddCore<T>::reduce((void*) funcP, OP_BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> collect( ){
			return fddCore<T>::context->collectRDD(this);
		}
		/*void * _collect( ) override{
			return fddCore<T>::context->collectRDD(this);
		}*/

};

template <class T> 
class fdd<T *> : public fddCore<T>{
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		fdd(fastContext &c) : fddCore<T>(c){ }

		// Create a empty fdd with a pre allocated size
		fdd(fastContext &c, size_t s) : fddCore<T>(c, s){ }

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
			return fddCore<T>::reduceP((void*) funcP, OP_Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return fddCore<T>::reduceP((void*) funcP, OP_BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T *> collect( ) {
			return this->context->collectRDD(this);
		}
		/*void * _collect( ) override{
			return fddCore<T>::context->collectRDD(this);
		}*/

};

#endif
