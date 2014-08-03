#ifndef LIBFASTER__WORKERFDD_H
#define LIBFASTER__WORKERFDD_H

#include <list>
#include <omp.h>
#include <unordered_map>

#include "workerFddBase.h"

namespace faster {

	template <class T>
	class fddStorage;

	template <class T>
	class workerFddCore;

	template <class T>
	class workerFdd;

	template <class T>
	class _workerFdd;

	template <class K, class T>
	class workerIFdd;


	template <class T>
	class workerFddCore : public workerFddBase{
		protected:
			fddStorage <T> * localData;

		public:
			workerFddCore(unsigned int ident, fddType t);
			workerFddCore(unsigned int ident, fddType t, size_t size);
			~workerFddCore();

			void setData(void * k UNUSED, void * d UNUSED, size_t size UNUSED){}

			void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t size UNUSED) override{}
			void setDataRaw(void * keys UNUSED, void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED) override{}

			fddType getType() override ;
			fddType getKeyType() override ;

			T & operator[](size_t address);
			void * getItem(size_t address){
				return &localData[address];
			}
			void * getKeys() override{
				return NULL;
			}
			void * getData() override;
			size_t getSize() override;
			size_t itemSize() override;
			size_t baseSize() override;
			void   setSize(size_t s);

			void deleteItem(void * item) override ;
			void shrink();

			void preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm);

	};

	// Worker side FDD
	template <class T>
	class _workerFdd : public workerFddCore<T>{
		private:
			// Not Pointer -> Not Pointer
			template <typename U>
			void _apply(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _applyP(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyI(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyIP(void * func, fddOpType op, workerFddBase * dest);
			void _applyReduce(void * func, fddOpType op, fastCommBuffer & buffer);

			template <typename L>
			void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
			void _preApply(void * func, fddOpType op, workerFddBase * dest);

			// --------- FUNCTIONS ----------

			template <typename U>
			void map (workerFddBase * dest, mapFunctionP<T,U> mapFunc);
			template <typename U>
			void map (workerFddBase * dest, PmapFunctionP<T,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, ImapFunctionP<T,L,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, IPmapFunctionP<T,L,U> mapFunc);

			template <typename U>
			void bulkMap (workerFddBase * dest, bulkMapFunctionP<T,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, PbulkMapFunctionP<T,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IbulkMapFunctionP<T,L,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IPbulkMapFunctionP<T,L,U> bulkMapFunc);

			template <typename U>
			void flatMap(workerFddBase * dest,  flatMapFunctionP<T,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  PflatMapFunctionP<T,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IflatMapFunctionP<T,L,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IPflatMapFunctionP<T,L,U> flatMapFunc );

			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  bulkFlatMapFunctionP<T,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  PbulkFlatMapFunctionP<T,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapFunctionP<T,L,U> bulkFlatMapFunc );




			// REDUCE
			T reduce (reduceFunctionP<T> reduceFunc);
			T bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc);


		public:

			_workerFdd(unsigned int ident, fddType t) : workerFddCore<T>(ident, t) {}
			_workerFdd(unsigned int ident, fddType t, size_t size) : workerFddCore<T>(ident, t, size) {}
			~_workerFdd(){}

			// For known types
			void setData(T * data, size_t size) ;
			void setData(void * d UNUSED, size_t size UNUSED){
				setData((T*) d, size);
			}
			void setData(void * d UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED){
				setData((T*) d, size);
			}
			void setData(void * k UNUSED, void * d UNUSED, size_t *lineSizes UNUSED, size_t size UNUSED){
				setData((T*) d, size);
			}

			// For anonymous types
			void setDataRaw(void * data, size_t size) override;
			void setDataRaw(void * data UNUSED, size_t * listSizes UNUSED, size_t size UNUSED ) override{}

			size_t * getLineSizes(){
				return NULL;
			}

			void insert(void * k, void * in, size_t s);
			void insertl(void * in); 

			void insert(T & in);
			void insert(T * in UNUSED, size_t s UNUSED){}
			void insert(std::list<T> & in);
			void insert(std::list<std::pair<T*,size_t>> & in UNUSED){}

			// Apply task functions to FDDs
			void apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);

			void collect(fastComm * comm) override;


	};

	// Pointer specialization
	template <class T>
	class _workerFdd<T*> : public workerFddCore<T*>{
		private:
			// Pointer -> Not Pointer
			template <typename U>
			void _apply(void * func, fddOpType op, workerFddBase * dest);
			template <typename U>
			void _applyP(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyI(void * func, fddOpType op, workerFddBase * dest);
			template <typename L, typename U>
			void _applyIP(void * func, fddOpType op, workerFddBase * dest);
			void _applyReduce(void * func, fddOpType op, fastCommBuffer & buffer);

			template <typename L>
			void _preApplyI(void * func, fddOpType op, workerFddBase * dest);
			void _preApply(void * func, fddOpType op, workerFddBase * dest);
			// --------- FUNCTIONS ----------

			template <typename U>
			void map (workerFddBase * dest, mapPFunctionP<T,U> mapFunc);
			template <typename U>
			void map (workerFddBase * dest, PmapPFunctionP<T,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, ImapPFunctionP<T,L,U> mapFunc);
			template <typename L, typename U>
			void map (workerFddBase * dest, IPmapPFunctionP<T,L,U> mapFunc);

			template <typename U>
			void bulkMap (workerFddBase * dest, bulkMapPFunctionP<T,U> bulkMapFunc);
			template <typename U>
			void bulkMap (workerFddBase * dest, PbulkMapPFunctionP<T,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IbulkMapPFunctionP<T,L,U> bulkMapFunc);
			template <typename L, typename U>
			void bulkMap (workerFddBase * dest, IPbulkMapPFunctionP<T,L,U> bulkMapFunc);

			template <typename U>
			void flatMap(workerFddBase * dest,  flatMapPFunctionP<T,U> flatMapFunc );
			template <typename U>
			void flatMap(workerFddBase * dest,  PflatMapPFunctionP<T,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IflatMapPFunctionP<T,L,U> flatMapFunc );
			template <typename L, typename U>
			void flatMap(workerFddBase * dest,  IPflatMapPFunctionP<T,L,U> flatMapFunc );

			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  bulkFlatMapPFunctionP<T,U> bulkFlatMapFunc );
			template <typename U>
			void bulkFlatMap(workerFddBase * dest,  PbulkFlatMapPFunctionP<T,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc );
			template <typename L, typename U>
			void bulkFlatMap(workerFddBase * dest,  IPbulkFlatMapPFunctionP<T,L,U> bulkFlatMapFunc );


			// REDUCE
			std::pair<T*,size_t> reduce (PreducePFunctionP<T> reduceFunc);
			std::pair<T*,size_t> bulkReduce (PbulkReducePFunctionP<T> bulkReduceFunc);
			

		public:
			_workerFdd(unsigned int ident, fddType t) : workerFddCore<T*>(ident, t) {}
			_workerFdd(unsigned int ident, fddType t, size_t size) : workerFddCore<T*>(ident, t, size) {}
			~_workerFdd(){}


			// For known types
			void setData(T ** data, size_t *lineSizes, size_t size) ;
			void setData(void * d UNUSED, size_t size UNUSED){ }
			void setData(void * data UNUSED, size_t * lineSizes UNUSED, size_t size UNUSED){
				setData((T**) data, lineSizes, size);
			}
			void setData(void * k UNUSED, void * d UNUSED, size_t *lineSizes UNUSED, size_t size UNUSED){
				setData((T**) d, lineSizes, size);
			}

			// For anonymous types
			void setDataRaw(void * data UNUSED, size_t size UNUSED) override {}
			void setDataRaw(void * data, size_t *lineSizes, size_t size) override;

			size_t * getLineSizes();

			void insert(void * k, void * in, size_t s);
			void insertl(void * in);

			void insert(T & in);
			void insert(T* & in, size_t s);
			void insert(std::list<T> & in);
			void insert(std::list< std::pair<T*, size_t> > & in);

			// Apply task functions to FDDs
			void apply(void * func, fddOpType op, workerFddBase * dest, fastCommBuffer & buffer);
			void collect(fastComm * comm) override;

	};
}

#endif
