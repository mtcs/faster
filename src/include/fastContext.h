#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <vector>
#include <queue>
#include <typeinfo>
#include <math.h>


#include "definitions.h"
#include "fddBase.h"
#include "fastComm.h"

template <class T> 
class fdd;

class fastContext;

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
		unsigned long int readFDD(fddBase * ref, const char * fileName);
		void getFDDInfo(size_t & size);
		int numProcs(){ return comm->numProcs; }
		

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);

		void * recvTaskResult(unsigned long int &id, size_t & size);
				

		template <class T> 
		std::vector<T> collectRDD(fdd<T> * fddP){
			size_t s = fddP->getSize();
			size_t recvData = 0, rsize;
			unsigned long int rid;
			std::vector<T> result(s);

			comm->sendCollect(id);

			while (recvData < s){// TODO put a timeout here
				comm->recvFDDData(rid, &(result.data())[recvData], rsize);
				recvData += rsize;
			}
			return result;
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

				comm->sendFDDSetIData(id, i, &keys[offset], &data[offset], dataPerProc * sizeof(T));
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
				std::cerr << "    S:FDDSetPData P" << i << " " << id << " " << dataPerProc << "B ";

				comm->sendFDDSetData(id, i, (void **) &data[offset], &dataSizes[offset], dataPerProc, sizeof(T));
				offset += dataPerProc;
				std::cerr << "!\n";
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
				std::cerr << "    S:FDDSetPDataI P" << i << " " << id << " " << dataPerProc << "B ";

				comm->sendFDDSetIData(id, i, &keys[offset],(void **) &data[offset], &dataSizes[offset], dataPerProc, sizeof(T));
				offset += dataPerProc;
				std::cerr << "!\n";
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
				std::cerr << "    S:FDDSetVData P" << i << " " << id << " " << dataPerProc << "B";

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
				std::cerr << "    S:FDDSetVData P" << i << " " << id << " " << dataPerProc << "B";

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

		void destroyFDD(unsigned long int id);

};



#endif
