#ifndef LIBFASTER_FDD_H
#define LIBFASTER_FDD_H

#include <vector>
#include <typeinfo>
#include <stdio.h>
#include <list>
#include <omp.h>


#include "definitions.h"
#include "fddBase.h"
#include "fastContext.h"

namespace faster{

	class fastTask;

	template <class K, class T> 
	class indexedFdd ; 

	template <typename T> 
	class fddCore : public fddBase {
		protected :
			fastContext * context;

			// -------------- Core FDD Functions --------------- //

			fddCore() {
				cached = false;
			}

			fddCore(fastContext & c);

			// Create a empty fdd with a pre allocated size
			fddCore(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc);

			// 1->1 function (map, bulkmap, flatmap...)
			fddBase * _map( void * funcP, fddOpType op, fddBase * newFdd);
			template <typename L, typename U> 
			indexedFdd<L,U> * mapI( void * funcP, fddOpType op);
			template <typename U> 
			fdd<U> * map( void * funcP, fddOpType op);

		public:

			void discard(){
				context->discardFDD(id);
			}
			void setKeyMap(void * keyMap UNUSED) {}
			void setGroupedByKey(bool gbk UNUSED) {}
	};

	// Driver side FDD
	// It just sends commands to the workers.
	template <class T> 
	class fdd : public fddCore<T>{
		private:
			T finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op);
			T reduce( void * funcP, fddOpType op);
		public:
			// -------------- Constructors --------------- //

			// Create a empty fdd
			fdd(fastContext &c) : fddCore<T>(c){ 
				this->id = c.createFDD(this,  typeid(T).hash_code());
			}

			// Create a empty fdd with a pre allocated size
			fdd(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) : fddCore<T>(c, s, dataAlloc){ 
				this->id = c.createFDD(this,  typeid(T).hash_code(), dataAlloc);
			}

			// Create a empty fdd with a pre allocated size
			fdd(fastContext &c, size_t s) : fdd(c, s, c.getAllocation(s)) { 
			}

			// Create a fdd from a array in memory
			fdd(fastContext &c, T * data, size_t size) : fdd(c, size){
				c.parallelize(fddBase::id, data, size); 
			}

			// Create a fdd from a vector in memory
			fdd(fastContext &c, std::vector<T> &dataV) : fdd(c, dataV.data(), dataV.size()){ }

			// Create a fdd from a file
			fdd(fastContext &c, const char * fileName) ;

			~fdd(){
			}

			// -------------- FDD Functions --------------- //

			// Run a Map
			template <typename U> 
			fdd<U> * map( mapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename U> 
			fdd<U> * map( PmapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( ImapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( IPmapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_Map);
			}


			template <typename U> 
			fdd<U> * bulkMap( bulkMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename U> 
			fdd<U> * bulkMap( PbulkMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IbulkMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IPbulkMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}


			template <typename U> 
			fdd<U> * flatMap( flatMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename U> 
			fdd<U> * flatMap( PflatMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IflatMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IPflatMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}


			template <typename U> 
			fdd<U> * bulkFlatMap( bulkFlatMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename U> 
			fdd<U> * bulkFlatMap( PbulkFlatMapFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}


			// Run a Reduce
			T reduce( reduceFunctionP<T> funcP ){
				return reduce((void*) funcP, OP_Reduce);
			}
			T bulkReduce( bulkReduceFunctionP<T> funcP ){
				return reduce((void*) funcP, OP_BulkReduce);
			}
			
			// --------------- FDD Builtin functions ------------- // 
			// Collect a FDD
			std::vector<T> collect( ){
				std::vector<T> data(this->size);
				this->context->collectFDD(data, this);
				return data;
			}
			/*void * _collect( ) override{
				return fddCore<T>::context->collectFDD(this);
			}*/
			fdd<T> * cache(){
				this->cached = true;
				return this;
			}

	};

	template <class T> 
	class fdd<T *> : public fddCore<T>{
		private:
			std::vector <T> finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op);
			std::vector <T> reduceP(void * funcP, fddOpType op);
		public:
			// -------------- Constructors --------------- //

			// Create a empty fdd
			fdd(fastContext &c) : fddCore<T>(c){ 
				this->id = c.createPFDD(this,  typeid(T).hash_code());
			}

			// Create a empty fdd with a pre allocated size
			fdd(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) : fddCore<T>(c, s, dataAlloc){ 
				this->id = c.createPFDD(this,  typeid(T).hash_code(), dataAlloc);
			}
			fdd(fastContext &c, size_t s) :fdd(c, s, c.getAllocation(s)) { }

			// Create a fdd from a array in memory
			fdd(fastContext &c, T * data[], size_t dataSizes[], size_t size) : fdd(c, size){
				c.parallelize(fddBase::id, data, dataSizes, size);
			}

			~fdd(){
			}


			// -------------- FDD Functions Parameter Specification --------------- //
			// These need to be specialized because they can return a pointer or not 

			// Run a Map
			template <typename U> 
			fdd<U> * map( mapPFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename U> 
			fdd<U> * map( PmapPFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( ImapPFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_Map);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * map( IPmapPFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_Map);
			}


			template <typename U> 
			fdd<U> * bulkMap( bulkMapPFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename U> 
			fdd<U> * bulkMap( PbulkMapPFunctionP<T,U> funcP ){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IbulkMapPFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkMap( IPbulkMapPFunctionP<T,L,U> funcP ){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkMap);
			}


			template <typename U> 
			fdd<U> * flatMap( flatMapPFunctionP<T,U> funcP){
				return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename U> 
			fdd<U> * flatMap( PflatMapPFunctionP<T,U> funcP){
				return fddCore<T>::template map<U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IflatMapPFunctionP<T,L,U> funcP){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * flatMap( IPflatMapPFunctionP<T,L,U> funcP){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_FlatMap);
			}


			template <typename U> 
			fdd<U> * bulkFlatMap( bulkFlatMapPFunctionP<T,U> funcP){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename U> 
			fdd<U> * bulkFlatMap( PbulkFlatMapPFunctionP<T,U> funcP){
				return fddCore<T>::template map<U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IbulkFlatMapPFunctionP<T,L,U> funcP){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename L, typename U> 
			indexedFdd<L,U> * bulkFlatMap( IPbulkFlatMapPFunctionP<T,L,U> funcP){
				return fddCore<T>::template mapI<L,U>((void*) funcP, OP_BulkFlatMap);
			}

			// Run a Reduce
			inline std::vector<T> reduce(PreducePFunctionP<T> funcP  ){
				return reduceP((void*) funcP, OP_Reduce);
			}
			inline std::vector<T> bulkReduce(PbulkReducePFunctionP<T> funcP  ){
				return reduceP((void*) funcP, OP_BulkReduce);
			}
			
			// --------------- FDD Builtin functions ------------- // 
			// Collect a FDD
			std::vector<std::pair<T*, size_t>> collect( ) {
				std::vector<std::pair<T*, size_t>> data(this->size);
				this->context->collectFDD(data, this);
				return data;
			}
			/*void * _collect( ) override{
				return fddCore<T>::context->collectFDD(this);
			}*/
			fdd<T*> * cache(){
				this->cached = true;
				return this;
			}

	};

	template <typename T>
	fddCore<T>::fddCore(fastContext & c){
		cached = false;
		context = &c;
	}

	template <typename T>
	fddCore<T>::fddCore(fastContext &c, size_t s, const std::vector<size_t> & dataAlloc) : fddCore(c){
		this->size = s;
		this->dataAlloc = dataAlloc;
	}


	template <typename T> 
	fddBase * fddCore<T>::_map( void * funcP, fddOpType op, fddBase * newFdd){
		unsigned long int tid, sid;
		//std::cerr << "  Map ";

		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);
		//std::cerr << " " << funcId << ".\n";

		// Send task
		auto start = system_clock::now();
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		auto result = context->recvTaskResult(tid, sid, start);

		if ( (op & 0xff) & (OP_FlatMap) ) {
			size_t newSize = 0;
			for (int i = 1; i < context->numProcs(); ++i){
				if (result[i].second > 0) newSize += * (size_t *) result[i].first;
			}

			newFdd->setSize(newSize);
		}

		if (!cached)
			this->discard();

		//std::cerr << "\n";
		return newFdd;
	}

	template <typename T> 
	template <typename U> 
	fdd<U> * fddCore<T>::map( void * funcP, fddOpType op){
		fddBase * newFdd ;

		if ( (op & 0xFF ) & (OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new fdd<U>(*context);
		}else{
			newFdd = new fdd<U>(*context, size, dataAlloc);
		}

		return (fdd<U> *) _map(funcP, op, newFdd);
	}
	template <typename T> 
	template <typename L, typename U> 
	indexedFdd<L,U> * fddCore<T>::mapI( void * funcP, fddOpType op){
		fddBase * newFdd ;

		if ( (op & 0xFF ) & (OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<L,U>(*context);
		}else{
			newFdd = new indexedFdd<L,U>(*context, size, dataAlloc);
		}

		return (indexedFdd<L,U> *) _map(funcP, op, newFdd);
	}

	template <typename T>
	T fdd<T>::finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op){
		fastCommBuffer buffer(0);
		T result;

		if (op == OP_Reduce){
			reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) this->context->funcTable[funcId];

			// Get the real object behind the buffer
			buffer.setBuffer(partResult[0], pSize[0]);
			buffer >> result;

			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				T pr;

				//std::cerr << "      Reduce Result Size:" << pSize[i] << "\n";
				if (pSize[i] == 0) std::cerr << "UNEXPECTED ERROR!!!!";

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> pr;

				result = reduceFunc(result, pr);
			}
		}else{
			bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) this->context->funcTable[funcId];
			T * vals = new T[this->context->numProcs() - 1];

			#pragma omp parallel for
			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				fastCommBuffer buffer(0);

				buffer.setBuffer(partResult[i], pSize[i]);
				buffer >> vals[i];
			}

			result = bulkReduceFunc(vals, this->context->numProcs() - 1);
			delete [] vals;
			// TODO do bulkreduce	
		}
		return result;
	}

	template <typename T>
	T fdd<T>::reduce( void * funcP, fddOpType op){
		//std::cerr << "  Reduce ";
		//T fddCore<T>::template reduce( int funcId, fddOpType op){
		T result;
		unsigned long int tid, sid;
		int funcId = this->context->findFunc(funcP);
		//std::cerr << " " << funcId << ".\n";
		char ** partResult = new char*[this->context->numProcs() - 1];
		size_t * rSize = new size_t[this->context->numProcs() - 1];

		// Send task
		//unsigned long int reduceTaskId = 
		auto start = system_clock::now();
		this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		auto resultV = this->context->recvTaskResult(tid, sid, start);

		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			partResult[i] = (char*) resultV[i + 1].first;
			rSize[i] = resultV[i + 1].second;
		}

		// Finish applying reduces
		result = finishReduces(partResult, rSize, funcId, op);

		delete [] partResult;
		delete [] rSize;

		if (!this->cached)
			this->discard();

		//std::cerr << "\n";
		return result;
	}

	template <typename T>
	std::vector<T> fdd<T*>::finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op){
		T * result; 
		size_t rSize; 

		if (op == OP_Reduce){
			T * pResult;
			size_t prSize;

			PreducePFunctionP<T> reduceFunc = ( PreducePFunctionP<T> ) this->context->funcTable[funcId];

			result = partResult[0];
			rSize = partrSize[0];

			// Do the rest of the reduces
			for (int i = 1; i < (this->context->numProcs() - 1); ++i){
				pResult = result;
				prSize = rSize;
				std::pair<T*,size_t> r = reduceFunc(pResult, prSize, partResult[i], partrSize[i]);
				result = r.first;
				rSize = r.second; 
			}

		}else{
			PbulkReducePFunctionP<T> bulkReduceFunc = (PbulkReducePFunctionP<T>) this->context->funcTable[funcId];
			std::pair<T*,size_t> r = bulkReduceFunc(partResult, partrSize, this->context->numProcs() - 1);
			result = r.first;
			rSize = r.second; 
		}

		std::vector<T> vResult(rSize);
		vResult.assign(result,  result + rSize);

		return vResult;
	}

	template <typename T>
	std::vector <T> fdd<T*>::reduceP(void * funcP, fddOpType op){
		//std::cerr << "  Reduce";
		//T fddCore<T>::template reduce( int funcId, fddOpType op){
		// Decode function pointer
		int funcId = this->context->findFunc(funcP);
		T ** partResult = new T*[this->context->numProcs() -1];
		size_t * partrSize = new size_t[this->context->numProcs() - 1];
		unsigned long int tid, sid;
		//std::cerr << " " << funcId << ".\n";

		// Send task
		//unsigned long int reduceTaskId = 
		auto start = system_clock::now();
		this->context->enqueueTask(op, this->id, 0, funcId, this->size);

		// Receive results
		auto resultV = this->context->recvTaskResult(tid, sid, start);

		for (int i = 0; i < (this->context->numProcs() - 1); ++i){
			partResult[i] = (T*) resultV[i + 1].first;
			partrSize[i] = resultV[i + 1].second /= sizeof(T);
		}


		// Finish applying reduces
		std::vector<T> vResult = finishPReduces(partResult, partrSize, funcId, op);

		delete [] partResult;
		delete [] partrSize;

		if (!this->cached)
			this->discard();

		return vResult;
	}


	template <typename T>
	fdd<T>::fdd(fastContext &c, const char * fileName) {
		fddCore<T>::context = &c;
		this->id = c.readFDD(this, fileName);

		// Recover FDD information (size, ? etc )
		this->context->getFDDInfo(this->size, this->dataAlloc);
	}


}

#endif
