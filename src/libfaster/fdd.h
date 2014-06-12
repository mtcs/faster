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
			id = c.createFDD( detectType( typeid(T).name() ) );
		}

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T * data, size_t size) {
			context = &c;
			id = c.createFDD( detectType( typeid(T).name() ) );
			c.parallelize(data, size, id);
		}

		// Create a fdd from a vector in memory
		template <typename U>
		fdd(U &c, std::vector<T> &data) {
			context = &c;
			id = c.createFDD( detectType( typeid(T).name() ) );
			c.parallelize(data.data(), data.length(), id);
		}

		// Create a fdd from a file
		template <typename U>
		fdd(U &c, const char * fileName) {
			context = &c;
			id = c.readFDD(fileName);
		}

		~fdd(){
		}

		// Run a Map
		template <typename U> fdd<U> * map( int funcId ){
			fdd<U> * newFdd = new fdd<U>(*context);
			unsigned long int newFddId = newFdd->id;

			context->enqueueTask(Map, id, newFddId, funcId);
			context->fddList.push_back( newFdd );

			return newFdd;
		}

		// Run a Reduce
		T reduce( int funcId ){
			T result;

			unsigned long int reduceTaskId = context->enqueueTask(Reduce, id, 0, funcId);
			result = context->collectTaskResult<T>(reduceTaskId);

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
