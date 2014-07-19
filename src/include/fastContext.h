#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <vector>
#include <queue>
#include <typeinfo>
#include <math.h>


template <typename T> 
class fdd;

template <typename K, typename T> 
class indexedFdd;

class fastTask;
class fastContext;

#include "definitions.h"
#include "fddBase.h"
#include "fastComm.h"


class fastSettings{
	friend class fastContext;
	public:

		fastSettings(const std::string &m, unsigned int b){
			master = m;
			blockSize = b;
		}
		fastSettings(const std::string m): fastSettings(m, 1024){ }
		fastSettings() : fastSettings("local", 1024){ }

		fastSettings(const fastSettings & s){
			master = s.master;
			blockSize = s.blockSize;
		}

		std::string getMaster() const{ return master; } 


	private:

	std::string master;
	unsigned int blockSize;

};

// General context
// Manages the driver and the Workers
class fastContext{

	template <class T> friend class fdd;
	template <class T> friend class fddCore;
	template <class K, class T> friend class iFddCore;
	template <class K, class T> friend class indexedFdd;
	friend class worker;

	public:
		fastContext( std::string m): fastContext(fastSettings(m)){}
		fastContext( const fastSettings & s);
		~fastContext();

		void registerFunction(void * funcP);

		void startWorkers();

	private:
		int id;
		unsigned long int numFDDs;
		unsigned long int numTasks;
		fastSettings * settings;
		//std::list< std::pair<void *, fddType> > fddList;
		std::vector< fddBase * > fddList;
		std::vector<void*> funcTable;
		fastComm * comm;

		std::vector<fastTask *> taskList;

		int findFunc(void * funcP);

		unsigned long int _createFDD(fddBase * ref, fddType type, size_t size);
		unsigned long int _createIFDD(fddBase * ref, fddType kTypeCode, fddType tTypeCode, size_t size);
		unsigned long int createFDD(fddBase * ref, size_t typeCode);
		unsigned long int createFDD(fddBase * ref, size_t typeCode, size_t size);
		unsigned long int createPFDD(fddBase * ref, size_t typeCode);
		unsigned long int createPFDD(fddBase * ref, size_t typeCode, size_t size);
		unsigned long int createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode);
		unsigned long int createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, size_t size);
		unsigned long int createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode);
		unsigned long int createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, size_t size);
		unsigned long int createFddGroup(fddBase * ref, std::vector<fddBase*> & fdds);

		unsigned long int readFDD(fddBase * ref, const char * fileName);
		void getFDDInfo(size_t & size);

		int numProcs(){ return comm->numProcs; }
		

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);
		unsigned long int enqueueTask(fddOpType opT, unsigned long int id);

		void * recvTaskResult(unsigned long int &id, size_t & size);
				

		template <typename Z, typename FDD> 
		void collectFDD(Z & ret, FDD * fddP){
			std::cerr << "    S:SendCollect " ;
			
			comm->sendCollect(fddP->getId());
			std::cerr << "ID:" << fddP->getId() << " ";

			comm->recvFDDDataCollect(ret);
			
			std::cerr << ".\n";
		}


		// Propagate FDD data to other machines
		// Primitive types
		template <typename T>
		void parallelize(unsigned long int id, T * data, size_t size){
			//int numBlocks = ceil ( size / settings->blockSize );
			//int blocksPerProc = numBlocks / (comm->numProcs - 1); // TODO DYNAMICALLY VARIATE BLOCK PER PROC LATER
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/(comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetData P" << i << " ID:" << id << " S:" << dataPerProc << "";

				comm->sendFDDSetData(id, i, &data[offset], dataPerProc * sizeof(T));
				offset += dataPerProc;
				std::cerr << ".\n";
			}
			comm->waitForReq(comm->numProcs - 1);
		}
		template <typename K, typename T>
		void parallelizeI(unsigned long int id, K * keys, T * data, size_t size){
			//int numBlocks = ceil ( size / settings->blockSize );
			//int blocksPerProc = numBlocks / (comm->numProcs - 1); // TODO DYNAMICALLY VARIATE BLOCK PER PROC LATER
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/(comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetDataI P" << i << " ID:" << id << " S:" << dataPerProc << "";

				comm->sendFDDSetIData(id, i, &keys[offset], &data[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
			comm->waitForReq(comm->numProcs - 1);
		}

		//Pointers
		template <typename T>
		void parallelize(unsigned long int id, T ** data, size_t * dataSizes, size_t size){
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetPData P" << i << " ID:" << id << " S:" << dataPerProc << "";

				comm->sendFDDSetData(id, i, &data[offset], &dataSizes[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
		}
		template <typename K, typename T>
		void parallelizeI(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size){
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetPDataI P" << i << " ID:" << id << " S:" << dataPerProc << "";

				comm->sendFDDSetIData(id, i, &keys[offset],  &data[offset], &dataSizes[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
		}
		//Containers
		template <typename T>
		void parallelizeC(unsigned long int id, T * data, size_t size ){
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetVData P" << i << " " << id << " " << dataPerProc << "";

				comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
			//comm->waitForReq(comm->numProcs - 1);
		}
		template <typename K, typename T>
		void parallelizeIC(unsigned long int id, K * keys, T * data, size_t size ){
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetVData P" << i << " " << id << " " << dataPerProc << "";

				comm->sendFDDSetIData(id, i, &keys[offset], &data[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
			//comm->waitForReq(comm->numProcs - 1);
		}
		template <typename T>
		void parallelize(unsigned long int id, std::vector<T> * data, size_t size ){
			parallelizeC(id, data, size);
		}
		void parallelize(unsigned long int id, std::string * data, size_t size){
			parallelizeC(id, data, size);
		}
		template <typename K, typename T>
		void parallelizeI(unsigned long int id, K * keys, std::vector<T> * data, size_t size ){
			parallelizeIC(id, keys, data, size);
		}
		template <typename K>
		void parallelizeI(unsigned long int id, K * keys, std::string * data, size_t size){
			parallelizeIC(id, keys, data, size);
		}

		template <typename K>
		CountKeyMapT<K> recvCountByKey(size_t size){
			CountKeyMapT<K> count(size);
			for ( int i = 0; i < (comm->numProcs - 1); ++i){
				comm->recvCountByKey(count);
			}
			return count;
		}

		void destroyFDD(unsigned long int id);

};



#endif
