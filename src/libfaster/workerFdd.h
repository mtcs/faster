#ifndef LIBFASTER_WORKERFDD_H
#define LIBFASTER_WORKERFDD_H

#include <omp.h>

template <class T>
class workerFdd;

#include "fastContext.h"
#include "fddStorage.h"

class workerFddBase{
	protected:
		unsigned long int id;
		fddType type;

	public:
		fddType getType(){ return type; }

};

// Worker side FDD
template <class T>
class workerFdd : public workerFddBase{
	private:
		fddStorage <T> localData;

	public:
		void setData(T * data, size_t size){
			localData.setData(data, size);
		}

		workerFdd(unsigned int ident, fddType t){
			id = ident;
			type = t;
		}

		T & operator[](size_t ref){
			return localData[ref];
		}

		T * getData(){
			return localData.getData();
		}
		size_t getSize(){
			return localData.getSize();
		}

		void insert(T & in){
			localData.insert(in);
		}

		void shrink(){
			localData.shrink();
		}

		// Real blocking processing functions
		template <typename U>
		void map (workerFdd<U> & dest, U (*mapFunc)(T & input)){
			size_t s = localData.getSize();

			#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				dest[i] = mapFunc(localData[i]);
			}
		}		

		template <typename U>
		void bulkMap (workerFdd<U> & dest, void (*bulkMapFunc)(U * output, T * input, size_t size)){
			size_t s = localData.getSize();

			bulkMapFunc(dest.getData(), localData.getData(), s);
		}

		void reduce (T &result, T (*reduceFunc)(T & A, T & B)){
			size_t s = localData.getSize();

			#pragma omp parallel 
			{
				int nT = omp_get_num_threads();
				int tN = omp_get_thread_num();
				T partResult = localData[tN];

				#pragma omp for 
				for (int i = nT; i < s; ++i){
					partResult = reduceFunc(partResult, localData[i]);
				}
				#pragma omp master
				result = partResult;
				
				#pragma omp barrier
				
				#pragma omp critical
				if (omp_get_thread_num() != 0){
					result = reduceFunc(result, partResult);
				}
			}
		}

		void bulkReduce (T &result, T (*bulkReduceFunc)(T * input, size_t size)){
			result = bulkReduceFunc(localData.getData(), localData.getSize());
		}
};

#endif
