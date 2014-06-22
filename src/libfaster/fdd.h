#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>

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

			int funcId = context->findFunc(funcP);
			//std::cerr << " " << funcId << ".\n";

			// Send task
			context->enqueueTask(op, id, newFddId, funcId);
			printf("1[%lx]", (size_t) context);fflush(stdout);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				context->recvTaskResult(id, &result, size);
			}
			printf("2[%lx]", (size_t) context);fflush(stdout);

			return newFdd;
		}

		T _reduce( void * funcP, fddOpType op){
		//T _reduce( int funcId, fddOpType op){
			T result;
			int funcId = context->findFunc(funcP);
			//std::cerr << " " << funcId << ".\n";

			// Send task
			printf("3[%lx]", (size_t) context);fflush(stdout);
			unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

			printf("4[%lx]", (size_t) context);fflush(stdout);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				unsigned long int id;
				T partResult;

				context->recvTaskResult(id, &partResult, size);
				if (size != sizeof(T)) std::cerr << "Error receiving result (wrong size)";

				// Do the rest of the reduces
				if( i == 1){
					result = partResult;
				}else{
					T (*reduceFunc)(T&, T&) = (T (*)(T&, T&)) context->funcTable[funcId];
					reduceFunc(result, partResult);
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
			c.parallelize(data, size, id);
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
		fdd<U> * map( void * funcP){
			return _map<U>(funcP, Map);
		}
		template <typename U> 
		fdd<U> * bulkMap( void * funcP){
			return _map<U>(funcP, BulkMap);
		}
		template <typename U> 
		fdd<U> * flatMap( void * funcP){
			return _map<U>(funcP, FlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( void * funcP){
			return _map<U>(funcP, BulkFlatMap);
		}

		// Run a Reduce
		T reduce( void * funcP ){
			return _reduce(funcP, Reduce);
		}
		T bulkReduce( void * funcP ){
			return _reduce(funcP, BulkReduce);
		}
		
		// Collect a FDD
		T * collect( ){
			return context->collectRDD<T>(id);
		}

};


#endif
