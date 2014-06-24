#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

template <class T> class fdd;
class fddBase;

#include "fastContext.h"
#include "fddStorage.h"
#include "misc.h"

// Driver side FDD
// It just sends commands to the workers.
template <class T> 
class fdd : public fddBase{
	private:
		fastContext * context;

		// -------------- Core FDD Functions --------------- //

		// 1->1 function (map, bulkmap, flatmap...)
		template <typename U> 
		fdd<U> * _map( void * funcP, fddOpType op){
		//fdd<U> * _map( int funcId, fddOpType op){
			fdd<U> * newFdd = new fdd<U>(*context, size);
			unsigned long int newFddId = newFdd->id;
			char result;
			size_t rSize;

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

		T _reduce( void * funcP, fddOpType op){
		//T _reduce( int funcId, fddOpType op){
			T result;
			int funcId = context->findFunc(funcP);
			//std::cerr << " " << funcId << ".\n";

			// Send task
			unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				unsigned long int id;
				T partResult;
				size_t rSize;

				context->recvTaskResult(id, &partResult, rSize);
				if (rSize != sizeof(T)) std::cerr << "Error receiving result (wrong size)";

				// Do the rest of the reduces
				if (op == Reduce){
					if( i == 1){
						result = partResult;
					}else{
							reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) context->funcTable[funcId];
							result = reduceFunc(result, partResult);
					}
				}else{
					// TODO do bulkreduce	
				}

			}

			return result;
		}

	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		fdd(U &c) {
			context = &c;
			id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fdd(U &c, size_t s) {
			context = &c;
			size = s;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
		}

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T * data, size_t size) : fdd(c, size){
			c.parallelize(id, data, size);
		}

		// Create a fdd from a vector in memory
		template <typename U>
		fdd(U &c, std::vector<T> &data) : fdd(c, data.data(),data.size()){ }

		// Create a fdd from a file
		template <typename U>
		fdd(U &c, const char * fileName) {
			context = &c;
			id = c.readFDD(this, fileName);

			// Recover FDD information (size, ? etc )
			context->getFDDInfo(size);
		}

		~fdd(){
		}

		// -------------- FDD Functions --------------- //

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( PmapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkFlatMap);
		}

		// TODO if it returns a pointer to U, specialize...

		// Run a Reduce
		T reduce( reduceFunctionP<T> funcP ){
			return _reduce((void*) funcP, Reduce);
		}
		T bulkReduce( bulkReduceFunctionP<T> funcP ){
			return _reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		T * collect( ){
			return context->collectRDD<T>(id);
		}

};

template <class T> 
class fdd<T *> : public fddBase{
	private:
		fastContext * context;

		// -------------- Core FDD Functions --------------- //

		// 1->1 function (map, bulkmap, flatmap...)
		template <typename U> 
		fdd<U> * _map( void * funcP, fddOpType op){
		//fdd<U> * _map( int funcId, fddOpType op){
			fdd<U> * newFdd = new fdd<U>(*context, size);
			unsigned long int newFddId = newFdd->id;
			char result;
			size_t rSize;

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

		std::vector <T> _reduce( void * funcP, fddOpType op){
		//T _reduce( int funcId, fddOpType op){
			// Decode function pointer
			int funcId = context->findFunc(funcP);
			T * result; 
			size_t rSize; 
			//std::cerr << " " << funcId << ".\n";

			// Send task
			unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				unsigned long int id;
				T * partResult;
				T * partResult2;
				size_t prSize;

				context->recvTaskResult(id, &partResult, prSize);

				// Do the rest of the reduces
				if( i == 1){
					result = partResult;
					rSize = prSize;
				}else{
					// TODO
					PreducePFunctionP<T> reduceFunc = ( PreducePFunctionP<T> ) context->funcTable[funcId];
					partResult2 = result;
					reduceFunc(result, rSize, partResult2, rSize, partResult, prSize);
					delete [] partResult;
					delete [] partResult2;
				}

			}
			std::vector<T> vResult(rSize);
			memcpy(vResult.data(), result, rSize);

			return vResult;
		}

	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		fdd(U &c) {
			context = &c;
			id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fdd(U &c, size_t s) {
			context = &c;
			size = s;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
		}

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T ** data, size_t * dataSizes, size_t size) : fdd(c, size){
			c.parallelize(id, data, dataSizes, size);
		}

		~fdd(){
		}

		// -------------- FDD Functions Parameter Specification --------------- //
		// These need to be specialized because they can return a pointer or not 

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapPFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( PmapPFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapPFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapPFunctionP<T,U> funcP ){
			return _map<U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapPFunctionP<T,U> funcP){
			return _map<U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapPFunctionP<T,U> funcP){
			return _map<U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapPFunctionP<T,U> funcP){
			return _map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapPFunctionP<T,U> funcP){
			return _map<U>((void*) funcP, BulkFlatMap);
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
		T * collect( ){
			return context->collectRDD<T>(id);
		}

};
#endif
