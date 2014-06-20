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
		workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) { }
		
		virtual ~workerFddBase(){}

		fddType getType(){ return type; }

		virtual void setData( void *, 	size_t){}
		
		virtual void * getData(){return NULL;}
		virtual size_t getSize(){return 0;}

		virtual size_t itemSize(){return 0;}

};

// Worker side FDD
template <class T>
class workerFdd : public workerFddBase{
	private:
		fddStorage <T> localData;

	public:
		void setData(void * data, size_t size) override{
			localData.setData(data, size);
		}

		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFddBase(ident, t){ 
			localData.setSize(size);
		}

		~workerFdd(){}

		T & operator[](size_t address){ return localData[address]; }
		void * getData() override{ return localData.getData(); }
		size_t getSize() override{ return localData.getSize(); }
		size_t itemSize() override{ return sizeof(T); }

		void insert(T & in){ localData.insert(in); }
		void shrink(){ localData.shrink(); }

		// --------- FUNCTIONS ----------

		// Real blocking processing functions
		template <typename U>
		void map (workerFdd<U> & dest, U (*mapFunc)(T & input)){
			size_t s = localData.getSize();

			std::cerr << "START " << id << " " << s << "  ";

			//#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				dest[i] = mapFunc(localData[i]);
			}
			std::cerr << "END ";
		}		

		template <typename U>
		void bulkMap (workerFdd<U> & dest, void (*bulkMapFunc)(U * output, T * input, size_t size)){
			size_t s = localData.getSize();

			bulkMapFunc(dest.getData(), localData.getData(), s);
		}

		void reduce (T &result, T (*reduceFunc)(T & A, T & B)){
			size_t s = localData.getSize();
			std::cerr << "START " << id << " " << s << " | ";

			//#pragma omp parallel 
			{
				//int nT = omp_get_num_threads();
				//int tN = omp_get_thread_num();
				//T partResult = localData[tN];
				T partResult = localData[0];

				//#pragma omp for 
				//for (int i = nT; i < s; ++i){
				for (int i = 1; i < s; ++i){
					partResult = reduceFunc(partResult, localData[i]);
					std::cerr << localData[i] << "->" << partResult << "\n";
				}
				//#pragma omp master
				result = partResult;
				
				//#pragma omp barrier
				
				//#pragma omp critical
				//if (omp_get_thread_num() != 0){
					//result = reduceFunc(result, partResult);
				//}
			}
			std::cerr << "END ";
		}

		void bulkReduce (T &result, T (*bulkReduceFunc)(T * input, size_t size)){
			result = bulkReduceFunc(localData.getData(), localData.getSize());
		}

};

#endif
