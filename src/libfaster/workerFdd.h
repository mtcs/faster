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
		workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {}
		
		virtual ~workerFddBase() {};

		virtual fddType getType() = 0;

		virtual void setData( void *, 	size_t) = 0;
		
		virtual void * getData() = 0;
		virtual size_t getSize() = 0;

		virtual size_t itemSize() = 0;

		virtual void apply(void * func, fddOpType op, workerFdd<char> * dest, void * result, size_t & rSize) = 0;
		virtual void apply(void * func, fddOpType op, workerFdd<int> * dest, void * result, size_t & rSize) = 0;
		virtual void apply(void * func, fddOpType op, workerFdd<long int> * dest, void * result, size_t & rSize) = 0;
		virtual void apply(void * func, fddOpType op, workerFdd<float> * dest, void * result, size_t & rSize) = 0;
		virtual void apply(void * func, fddOpType op, workerFdd<double> * dest, void * result, size_t & rSize) = 0;
		virtual void apply(void * func, fddOpType op, workerFdd<std::basic_string<char>> * dest, void * result, size_t & rSize) = 0;
		/*
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<char> * dest = NULL, void * result = NULL) = 0;
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<int> * dest = NULL, void * result = NULL) = 0;
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<long int> * dest = NULL, void * result = NULL) = 0;
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<float> * dest = NULL, void * result = NULL) = 0;
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<double> * dest = NULL, void * result = NULL) = 0;
		virtual void apply(void * func = NULL, fddOpType op = Map, workerFdd<std::string> * dest = NULL, void * result = NULL) = 0;
		*/
		
};

// Worker side FDD
template <class T>
class workerFdd : public workerFddBase{
	private:
		fddStorage <T> localData;

		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t & rSize){
			switch (op){
				case Map:
					map(*dest, ( U (*)(T &) ) func);
					std::cerr << "Map";
					rSize = sizeof(char);
					break;
				case BulkMap:
					bulkMap(*dest, ( void (*)(U *,T *, size_t) ) func);
					std::cerr << "BulkMap";
					rSize = sizeof(char);
					break;
				case FlatMap:
					std::cerr << "FlatMap";
					rSize = sizeof(char);
					break;
				case BulkFlatMap:
					std::cerr << "BulkFlatMap";
					rSize = sizeof(char);
					break;
				case Reduce:
					reduce( (T*) result, ( T (*)(T&, T&) ) func);
					std::cerr << "Reduce " << result << " ";
					rSize = sizeof(T);
					break;
				case BulkReduce:
					bulkReduce( (T*) result, ( T (*)(T*, size_t) ) func);
					std::cerr << "BulkReduce " << result << " ";
					rSize = sizeof(T);
					break;
			}
		}

		// --------- FUNCTIONS ----------

		// Real blocking processing functions
		template <typename U>
		void map (workerFdd<U> & dest, U (*mapFunc)(T & input)){
			size_t s = localData.getSize();

			//std::cerr << "START " << id << " " << s << "  ";

			#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				dest[i] = mapFunc(localData[i]);
			}
			//std::cerr << "END ";
		}		

		template <typename U>
		void bulkMap (workerFdd<U> & dest, void (*bulkMapFunc)(U * output, T * input, size_t size)){
			size_t s = localData.getSize();

			bulkMapFunc((U *)dest.getData(), (T *)localData.getData(), s);
		}

		void reduce (T *result, T (*reduceFunc)(T & A, T & B)){
			size_t s = localData.getSize();
			//std::cerr << "START " << id << " " << s << " | ";

			#pragma omp parallel 
			{
				int nT = omp_get_num_threads();
				int tN = omp_get_thread_num();
				T partResult = localData[tN];

				#pragma omp master
				std::cerr << tN << "(" << nT << ")";

				#pragma omp for 
				for (int i = nT; i < s; ++i){
					partResult = reduceFunc(partResult, localData[i]);
				}
				#pragma omp master
				*result = partResult;
				
				#pragma omp barrier
				
				#pragma omp critical
				if (omp_get_thread_num() != 0){
					*result = reduceFunc(*result, partResult);
				}
			}
			//std::cerr << "END ";
		}

		void bulkReduce (T *result, T (*bulkReduceFunc)(T * input, size_t size)){
			*result = bulkReduceFunc((T*) localData.getData(), localData.getSize());
		}


	public:
		void setData(void * data, size_t size) override{
			localData.setData(data, size);
		}
		fddType getType() override { return type; }

		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFdd(ident, t){ 
			localData.setSize(size);
		}

		~workerFdd(){}

		T & operator[](size_t address){ return localData[address]; }
		void * getData() override{ return localData.getData(); }
		size_t getSize() override{ return localData.getSize(); }
		size_t itemSize() override{ return sizeof(T); }

		void insert(T & in){ localData.insert(in); }
		void shrink(){ localData.shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFdd<char> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }
		void apply(void * func, fddOpType op, workerFdd<int> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }
		void apply(void * func, fddOpType op, workerFdd<long int> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }
		void apply(void * func, fddOpType op, workerFdd<float> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }
		void apply(void * func, fddOpType op, workerFdd<double> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }
		void apply(void * func, fddOpType op, workerFdd<std::basic_string<char>> * dest, void * result, size_t & rSize){ _apply(func, op, dest, result, rSize); }

};

#endif
