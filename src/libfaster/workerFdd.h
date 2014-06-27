#ifndef LIBFASTER_WORKERFDD_H
#define LIBFASTER_WORKERFDD_H

#include <list>
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

		virtual void setData( void *, size_t) = 0;
		virtual void setData( void **, size_t *, size_t) = 0;
		
		virtual void * getData() = 0;
		virtual size_t getSize() = 0;

		virtual size_t itemSize() = 0;
		virtual size_t baseSize() = 0;

		virtual void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize) = 0;
				
};

// Worker side FDD
template <class T>
class workerFdd : public workerFddBase{
	private:
		fddStorage <T> localData;

		// Not Pointer -> Not Pointer
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t rSize){
			switch (op){
				case OP_Map:
					map(*dest, (mapFunctionP<T,U>) func);
					std::cerr << "Map ";
					break;
				case OP_BulkMap:
					bulkMap(*dest, ( bulkMapFunctionP<T,U> ) func);
					std::cerr << "BulkMap ";
					break;
				case OP_FlatMap:
					flatMap(*dest, ( flatMapFunctionP<T,U> ) func);
					std::cerr << "FlatMap ";
					break;
				case OP_BulkFlatMap:
					bulkFlatMap(*dest, ( bulkFlatMapFunctionP<T,U> ) func);
					std::cerr << "BulkFlatMap ";
					break;
				case OP_Reduce:
					*((T*)result) = reduce( ( reduceFunctionP<T> ) func);
					std::cerr << "Reduce ";
					break;
				case OP_BulkReduce:
					*((T*)result) = bulkReduce( ( bulkReduceFunctionP<T> ) func);
					std::cerr << "BulkReduce ";
					break;
			}
		}

		// Not Pointer -> Pointer
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t rSize){
			switch (op){
				case OP_Map:
					map(*dest, (PmapFunctionP<T,U>) func);
					std::cerr << "Map ";
					break;
				case OP_BulkMap:
					bulkMap(*dest, ( PbulkMapFunctionP<T,U> ) func);
					std::cerr << "BulkMap ";
					break;
				case OP_FlatMap:
					flatMap(*dest, ( PflatMapFunctionP<T,U> ) func);
					std::cerr << "FlatMap ";
					break;
				case OP_BulkFlatMap:
					bulkFlatMap(*dest, ( PbulkFlatMapFunctionP<T,U> ) func);
					std::cerr << "BulkFlatMap ";
					break;
			}
		}

		// --------- FUNCTIONS ----------

		// MAP
		template <typename U>
		void map (workerFdd<U> & dest, mapFunctionP<T,U> mapFunc){
			size_t s = localData.getSize();

			//std::cerr << "START " << id << " " << s << "  ";

			#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				dest[i] = mapFunc(localData[i]);
			}
			//std::cerr << "END ";
		}		
		
		template <typename U>
		void map (workerFdd<U> & dest, PmapFunctionP<T,U> mapFunc){
			size_t s = localData.getSize();
			size_t * ls = dest.getLineSizes() ;

			//std::cerr << "START " << id << " " << s << "  ";

			#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				U result;
				size_t rSize;
				mapFunc(dest[i], ls[i], localData[i]);
			}
			//std::cerr << "END ";
		}		



		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapFunctionP<T,U> bulkMapFunc){
			bulkMapFunc((U*) dest.getData(), (T *)localData.getData(), localData.getSize());
		}
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapFunctionP<T,U> bulkMapFunc){
			bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (T *) localData.getData(), localData.getSize());
		}


		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapFunctionP<T,U> flatMapFunc ){
			size_t s = localData.getSize();
			std::list<U> resultList;

			#pragma omp parallel 
			{
				std::list<U> partResultList;

				#pragma omp for 
				for (int i = 0; i < s; ++i){
					std::list<U> r = flatMapFunc(localData[i]);

					partResultList.insert(partResultList.end(), r.begin(), r.end());
				}

				#pragma omp critical
				resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

			}
			dest.insert<U>(resultList);
		}
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapFunctionP<T,U> flatMapFunc ){
			size_t s = localData.getSize();
			std::list< std::pair<U, size_t> > resultList;

			#pragma omp parallel 
			{
				std::list<std::pair<U, size_t>> partResultList;

				#pragma omp for 
				for (int i = 0; i < s; ++i){
					std::list<std::pair<U, size_t>> r = flatMapFunc(localData[i]);

					partResultList.insert(partResultList.end(), r.begin(), r.end());
				}

				#pragma omp critical
				resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );

			}
			dest.insert<U>(resultList);
		}

		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
			U * result;
			size_t rSize;

			bulkFlatMapFunc(result, rSize, localData.getData(), localData.getSize());
			dest.setData(result, rSize);
		}
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapFunctionP<T,U> bulkFlatMapFunc ){
			U * result;
			size_t * rDataSizes;
			size_t rSize;

			bulkFlatMapFunc( result, rDataSizes, rSize, (T*) localData.getData(), localData.getSize());
			dest.setData( (void**) result, rDataSizes, rSize);
		}

		// REDUCE
		T reduce (reduceFunctionP<T> reduceFunc);

		T bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc);


	public:
		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFdd(ident, t){ 
			localData.setSize(size);
		}

		~workerFdd(){}

		void setData(void * data, size_t size) override{
			localData.setData(data, size);
		}
		void setData(void ** data, size_t *lineSizes, size_t size) override{ }
		fddType getType() override { return type; }

		T & operator[](size_t address){ return localData[address]; }
		void * getData() override{ return localData.getData(); }
		size_t getSize() override{ return localData.getSize(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T); }

		void insert(T & in){ localData.insert(in); }

		template <typename U>
		void insert(std::list<T> & in){ 
			typename std::list<U>::iterator it;

			if (localData.getSize() < in.size())
				localData.grow(in.size());

			for ( it = in.begin(); it != in.end(); it++)
				localData.insert(*it); 
		}

		void shrink(){ localData.shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize);

};

// Pointer specialization
template <class T>
class workerFdd<T *> : public workerFddBase{
	private:
		fddStorage <T *> localData;

		// Pointer -> Not Pointer
		template <typename U>
		void _apply(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t & rSize){
			switch (op){
				case OP_Map:
					map(*dest, (mapPFunctionP<T,U>) func);
					std::cerr << "Map";
					break;
				case OP_BulkMap:
					bulkMap(*dest, ( bulkMapPFunctionP<T,U> ) func);
					std::cerr << "BulkMap ";
					break;
				case OP_FlatMap:
					flatMap(*dest, ( flatMapPFunctionP<T,U> ) func);
					std::cerr << "FlatMap ";
					break;
				case OP_BulkFlatMap:
					bulkFlatMap(*dest, ( bulkFlatMapPFunctionP<T,U> ) func);
					std::cerr << "BulkFlatMap ";
					break;
			}
		}
		
		// Pointer -> Pointer
		template <typename U>
		void _applyP(void * func, fddOpType op, workerFdd<U> * dest, void * result, size_t & rSize){
			switch (op){
				case OP_Map:
					map(*dest, (PmapPFunctionP<T,U>) func);
					std::cerr << "Map";
					break;
				case OP_BulkMap:
					bulkMap(*dest, ( PbulkMapPFunctionP<T,U> ) func);
					std::cerr << "BulkMap ";
					break;
				case OP_FlatMap:
					flatMap(*dest, ( PflatMapPFunctionP<T,U> ) func);
					std::cerr << "FlatMap ";
					break;
				case OP_BulkFlatMap:
					bulkFlatMap(*dest, ( PbulkFlatMapPFunctionP<T,U> ) func);
					std::cerr << "BulkFlatMap ";
					break;
				case OP_Reduce:
					result = reduce(rSize, ( PreducePFunctionP<T> ) func);
					std::cerr << "Reduce " ;
					break;
				case OP_BulkReduce:
					result = bulkReduce(rSize, ( PbulkReducePFunctionP<T> ) func);
					std::cerr << "BulkReduce ";
					break;
			}
		}

		// --------- FUNCTIONS ----------
		// MAP
		template <typename U>
		void map (workerFdd<U> & dest, mapPFunctionP<T,U> mapFunc){
			size_t s = localData.getSize();
			size_t * ls = localData.getLineSizes();

			//std::cerr << "START " << id << " " << s << "  ";

			#pragma omp parallel for 
			for (int i = 0; i < s; ++i){
				dest[i] = mapFunc(localData[i], ls[i]);
			}
			//std::cerr << "END ";
		}
		
		template <typename U>
		void map (workerFdd<U> & dest, PmapPFunctionP<T,U> mapFunc){
			size_t s = localData.getSize();
			size_t * ls = localData.getLineSizes();
			size_t * dls = dest.getLineSizes();
			//std::cerr << "START " << id << " " << s << "  ";

			#pragma omp parallel for 
			for (int i = 0; i < localData.getSize(); ++i){
				mapFunc(dest[i], dls[i], localData[i], ls[i]);
			}
			//std::cerr << "END ";
		}		

		template <typename U>
		void bulkMap (workerFdd<U> & dest, bulkMapPFunctionP<T,U> bulkMapFunc){
			size_t s = localData.getSize();
			size_t * ls = localData.getLineSizes();

			bulkMapFunc((U*) dest.getData(), (T **)localData.getData(), ls, s);
		}
		template <typename U>
		void bulkMap (workerFdd<U> & dest, PbulkMapPFunctionP<T,U> bulkMapFunc){
			size_t s = localData.getSize();
			size_t * ls = localData.getLineSizes();

			bulkMapFunc((U*) dest.getData(), dest.getLineSizes(), (T **)localData.getData(), ls, s);
		}

		template <typename U>
		void flatMap(workerFdd<U> & dest,  flatMapPFunctionP<T,U> flatMapFunc ){
			size_t s = localData.getSize();
			std::list<U> resultList;

			#pragma omp parallel
			{
				std::list<U> partResultList;
				#pragma omp for 
				for (int i = 0; i < s; ++i){
					std::list<U>r = flatMapFunc(localData[i], localData.getLineSizes()[i]);

					resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
				}

				//Copy result to the FDD array
				#pragma omp critical
				resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
			}
			dest.insert<U>(resultList);
		}
		template <typename U>
		void flatMap(workerFdd<U> & dest,  PflatMapPFunctionP<T,U> flatMapFunc ){
			size_t s = localData.getSize();
			std::list<std::pair<U, size_t>> resultList;

			#pragma omp parallel
			{
				std::list<std::pair<U, size_t>> partResultList;
				#pragma omp for 
				for (int i = 0; i < s; ++i){
					std::list<std::pair<U, size_t>>r = flatMapFunc(localData[i], localData.getLineSizes()[i]);

					resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
				}

				//Copy result to the FDD array
				#pragma omp critical
				resultList.insert(resultList.end(), partResultList.begin(), partResultList.end() );
			}
			dest.insert<U>(resultList);
		}

		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  bulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
			U * result;
			size_t rSize;

			bulkFlatMapFunc( result, rSize, (T**) localData.getData(), localData.getLineSizes(), localData.getSize());
			dest.setData(result, rSize);
		}
		template <typename U>
		void bulkFlatMap(workerFdd<U> & dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc ){
			U * result;
			size_t * rDataSizes;
			size_t rSize;

			bulkFlatMapFunc( result, rDataSizes, rSize, (T**) localData.getData(), localData.getLineSizes(), localData.getSize());
			dest.setData(result, rSize);
		}

		// REDUCE
		T * reduce (size_t & rSize, PreducePFunctionP<T> reduceFunc);

		T * bulkReduce (size_t & rSize, PbulkReducePFunctionP<T> bulkReduceFunc);
		

	public:
		workerFdd(unsigned int ident, fddType t) : workerFddBase(ident, t){} 

		workerFdd(unsigned int ident, fddType t, size_t size) : workerFdd(ident, t){ 
			localData.setSize(size);
		}

		~workerFdd(){}


		void setData(void * data, size_t size) override{
			//localData.setData(data, size);
		}
		void setData(void ** data, size_t *lineSizes, size_t size) override{
			localData.setData(data, lineSizes, size);
		}
		fddType getType() override { return type; }

		T *& operator[](size_t address){ return localData[address]; }
		void * getData() override{ return localData.getData(); }
		size_t getSize() override{ return localData.getSize(); }
		size_t * getLineSizes(){ return localData.getLineSizes(); }
		size_t itemSize() override{ return sizeof(T); }
		size_t baseSize() override{ return sizeof(T*); }

		void insert(T* & in, size_t s){ localData.insert(in, s); }

		template <typename U>
		void insert(std::list< std::pair<T*, size_t> > & in){ 
			typename std::list< std::pair<T*, size_t> >::iterator it;

			if (localData.getSize() < in.size())
				localData.grow(in.size());

			for ( it = in.begin(); it != in.end(); it++)
				localData.insert(it->first, it->second); 
		}

		void shrink(){ localData.shrink(); }


		// Apply task functions to FDDs
		void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize);
};

#endif
