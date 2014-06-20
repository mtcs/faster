#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>

template <class T> class fdd;
class fddBase;

#include "fastContext.h"
#include "fddStorage.h"
#include "misc.h"

// Driver side FDD
// It just sends commands to the workers.
template <class T> class fdd : public fddBase{
	public:
		// Create a empty fdd
		template <typename U>
		fdd(U &c) {
			context = &c;
			id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fdd(U &c, size_t size) {
			context = &c;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
			this->size = size;
		}

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T * data, size_t size) {
			context = &c;
			id = c.createFDD(this,  typeid(T).hash_code(), size);
			this->size = size;
			c.parallelize(data, size, id);
		}

		// Create a fdd from a vector in memory
		template <typename U>
		fdd(U &c, std::vector<T> &data) {
			context = &c;
			id = c.createFDD(this,  typeid(T).hash_code(), data.size() );
			this->size = data.size();
			c.parallelize(data.data(), data.length(), id);
		}

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

		// Run a Map
		template <typename U> fdd<U> * map( int funcId ){
			fdd<U> * newFdd = new fdd<U>(*context, size);
			unsigned long int newFddId = newFdd->id;
			char result;

			// Send task
			context->enqueueTask(Map, id, newFddId, funcId);

			// Receive results
			for (int i = 1; i < context->numProcs(); ++i){
				context->recvTaskResult(id, &result, size);
			}

			return newFdd;
		}


		// Run a Reduce
		T reduce( int funcId ){
			T result;

			// Send task
			unsigned long int reduceTaskId = context->enqueueTask(Reduce, id, 0, funcId);

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

		// Collect a FDD
		T * collect( ){
			return context->collectRDD<T>(id);
		}

	private:
		fastContext * context;
};


#endif
