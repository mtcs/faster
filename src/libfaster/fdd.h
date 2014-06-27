#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>

#include "fddBase.h"
#include "fastContext.h"
#include "fddStorage.h"

template <typename T> 
class fddCore : public fddBase {
	protected :
		fastContext * context;

		// -------------- Core FDD Functions --------------- //

	public :
		fddCore() {}

		template <typename U>
		fddCore(U &c) {
			fddCore<T>::context = &c;
			fddBase::id = c.createFDD(this, typeid(T).hash_code() );
		}

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fddCore(U &c, size_t s) {
			fddCore<T>::context = &c;
			fddBase::size = s;
			fddBase::id = c.createFDD(this,  typeid(T).hash_code(), s);
		}

		// 1->1 function (map, bulkmap, flatmap...)
		template <typename U> 
		fdd<U> * map( void * funcP, fddOpType op){
			fdd<U> * newFdd = new fdd<U>(*context, size);
			unsigned long int newFddId = newFdd->getId();
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

		T finishReduces(T * partResult, int funcId, fddOpType op){
			T result;
			if (op == Reduce){
				reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) context->funcTable[funcId];
				result = partResult[0];
				for (int i = 1; i < (context->numProcs() - 1); ++i){
					result = reduceFunc(result, partResult[i]);
				}
			}else{
				bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) context->funcTable[funcId];
				result = bulkReduceFunc(partResult, context->numProcs() - 1);
				// TODO do bulkreduce	
			}
			return result;
		}

		std::vector<T> finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op){
			T * result; 
			size_t rSize; 

			if (op == Reduce){
				T * pResult;
				size_t prSize;
				
				PreducePFunctionP<T> reduceFunc = ( PreducePFunctionP<T> ) context->funcTable[funcId];

				result = partResult[0];
				rSize = partrSize[0];

				// Do the rest of the reduces
				for (int i = 1; i < (context->numProcs() - 1); ++i){
					// TODO
					pResult = result;
					prSize = rSize;
					reduceFunc(result, rSize, pResult, prSize, partResult[i], partrSize[i]);
					delete [] pResult;
				}

			}else{
				PbulkReducePFunctionP<T> bulkReduceFunc = (PbulkReducePFunctionP<T>) context->funcTable[funcId];
				bulkReduceFunc(result, rSize, partResult, partrSize, context->numProcs() - 1);
			}
			
			std::vector<T> vResult(rSize);
			vResult.assign(result,  result + rSize);

			return vResult;
		}


		T reduce( void * funcP, fddOpType op){
		//T fddCore<T>::template reduce( int funcId, fddOpType op){
			T result;
			int funcId = context->findFunc(funcP);
			//std::cerr << " " << funcId << ".\n";
			T * partResult = new T[context->numProcs() - 1];
			size_t rSize;
			unsigned long int id;

			// Send task
			unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

			// Receive results
			for (int i = 0; i < (context->numProcs() - 1); ++i){
				context->recvTaskResult(id, &partResult[i], rSize);
			}

			// Finish applying reduces
			finishReduces(partResult, funcId, op);


			delete [] partResult;
			return result;
		}

		std::vector <T> reduceP(void * funcP, fddOpType op){
		//T fddCore<T>::template reduce( int funcId, fddOpType op){
			// Decode function pointer
			int funcId = context->findFunc(funcP);
			T ** partResult = new T*[context->numProcs() -1];
			size_t * partrSize = new size_t[context->numProcs() - 1];
			unsigned long int id;
			//std::cerr << " " << funcId << ".\n";

			// Send task
			unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);
			
			// Receive results
			for (int i = 0; i < (context->numProcs() - 1); ++i){
				context->recvTaskResult(id, &partResult[i], partrSize[i]);
			}

			// Finish applying reduces
			std::vector<T> vResult = finishPReduces(partResult, partrSize, funcId, op);

			delete [] partResult;
			delete [] partrSize;
			return vResult;
		}

};

// Driver side FDD
// It just sends commands to the workers.
template <class T> 
class fdd : public fddCore<T>{
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		fdd(U &c) : fddCore<T>(c){ }

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fdd(U &c, size_t s) : fddCore<T>(c, s){ }

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T * data, size_t size) : fdd(c, size){
			c.parallelize(fddBase::id, data, size);
		}

		// Create a fdd from a vector in memory
		template <typename U>
		fdd(U &c, std::vector<T> &data) : fdd(c, data.data(), data.size()){ }

		// Create a fdd from a file
		template <typename U>
		fdd(U &c, const char * fileName) {
			fddCore<T>::context = &c;
			fddBase::id = c.readFDD(this, fileName);

			// Recover FDD information (size, ? etc )
			fddCore<T>::context->getFDDInfo(fddBase::size);
		}

		~fdd(){
		}

		// -------------- FDD Functions --------------- //

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( PmapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkFlatMap);
		}

		// TODO if it returns a pointer to U, specialize...

		// Run a Reduce
		T reduce( reduceFunctionP<T> funcP ){
			return fddCore<T>::reduce((void*) funcP, Reduce);
		}
		T bulkReduce( bulkReduceFunctionP<T> funcP ){
			return fddCore<T>::reduce((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> * collect( ){
			return fddCore<T>::context->collectRDD(this);
		}
		void * _collect( ) override{
			return fddCore<T>::context->collectRDD(this);
		}

};

template <class T> 
class fdd<T *> : public fddCore<T>{
	public:
		// -------------- Constructors --------------- //

		// Create a empty fdd
		template <typename U>
		fdd(U &c) : fddCore<T>(c){ }

		// Create a empty fdd with a pre allocated size
		template <typename U>
		fdd(U &c, size_t s) : fddCore<T>(c, s){ }

		// Create a fdd from a array in memory
		template <typename U>
		fdd(U &c, T ** data, size_t * dataSizes, size_t size) : fdd(c, size){
			c.parallelize(fddBase::id, data, dataSizes, size);
		}

		~fdd(){
		}


		// -------------- FDD Functions Parameter Specification --------------- //
		// These need to be specialized because they can return a pointer or not 

		// Run a Map
		template <typename U> 
		fdd<U> * map( mapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, Map);
		}
		template <typename U> 
		fdd<U> * map( PmapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, Map);
		}


		template <typename U> 
		fdd<U> * bulkMap( bulkMapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkMap);
		}
		template <typename U> 
		fdd<U> * bulkMap( PbulkMapPFunctionP<T,U> funcP ){
			return fddCore<T>::template map<U>((void*) funcP, BulkMap);
		}


		template <typename U> 
		fdd<U> * flatMap( flatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, FlatMap);
		}
		template <typename U> 
		fdd<U> * flatMap( PflatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, FlatMap);
		}


		template <typename U> 
		fdd<U> * bulkFlatMap( bulkFlatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, BulkFlatMap);
		}
		template <typename U> 
		fdd<U> * bulkFlatMap( PbulkFlatMapPFunctionP<T,U> funcP){
			return fddCore<T>::template map<U>((void*) funcP, BulkFlatMap);
		}

		// Run a Reduce
		inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
			return fddCore<T>::reduceP((void*) funcP, Reduce);
		}
		inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
			return fddCore<T>::reduceP((void*) funcP, BulkReduce);
		}
		
		// --------------- FDD Builtin functions ------------- // 
		// Collect a FDD
		std::vector<T> * collect( ) {
			return fddCore<T>::context->collectRDD(this);
		}
		void * _collect( ) override{
			return fddCore<T>::context->collectRDD(this);
		}

};

#endif
