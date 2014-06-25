#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <vector>
#include <queue>
#include <typeinfo>
#include <math.h>


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
	friend class worker;

	public:
		fastContext( std::string m): fastContext(fastSettings(m)){}
		fastContext( const fastSettings & s);
		~fastContext();

		void registerFunction(void * funcP){
			//std::cerr << "  Register " << funcP ;
			funcTable.insert(funcTable.end(), funcP);
			//std::cerr << ".\n";
		}

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

		unsigned long int createFDD(fddBase * ref, size_t typeCode);
		unsigned long int createFDD(fddBase * ref, size_t typeCode, size_t size);
		unsigned long int readFDD(fddBase * ref, const char * fileName);
		void getFDDInfo(size_t & size);
		int numProcs(){ return comm->numProcs; }
		

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);

		void recvTaskResult(unsigned long int &id, void * result, size_t & size);
				

		template <class T> 
		std::vector<T> * collectRDD(fdd<T> * fddP){
			size_t s = fddP->getSize();
			T * data = new T[s];
			size_t recvData = 0, rsize;
			unsigned long int rid;

			comm->sendCollect(id);

			while (recvData < s){// TODO put a timeout here
				comm->recvFDDData(rid, &data[recvData], rsize);
				recvData += rsize;
			}
		}
		template <class T> 
		std::vector<T> * collectRDD(unsigned long int id){
			return (std::vector<T> *) fddList[id]->_collect();
		}



		// Propagate FDD data to other machines
		template <typename T>
		void parallelize(unsigned long int id, T * data, size_t size){
			//int numBlocks = ceil ( size / settings->blockSize );
			//int blocksPerProc = numBlocks / (comm->numProcs - 1); // TODO DYNAMICALLY VARIATE BLOCK PER PROC LATER
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetData P" << i << " " << id << " " << dataPerProc * sizeof(T) << "B";

				comm->sendFDDSetData(id, i, &data[offset], dataPerProc * sizeof(T));
				offset += dataPerProc;
				std::cerr << ".\n";
			}
			comm->waitForReq(comm->numProcs - 1);
		}

		template <typename T>
		void parallelize(unsigned long int id, T ** data, size_t * dataSizes, size_t size){
			int sizePerProc = size/ (comm->numProcs - 1);
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetData P" << i << " " << id << " " << sizePerProc * sizeof(T) << "B";

				comm->sendFDDSetData(id, i, (void **) &data[offset], &dataSizes[offset], dataPerProc, sizeof(T));
				offset += dataPerProc;
				std::cerr << ".\n";
			}
		}

		void parallelize(unsigned long int id, std::string * data, size_t size){
			int sizePerProc = size/ (comm->numProcs - 1);
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetData P" << i << " " << id << " " << sizePerProc << "B";

				comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
		}

		template <typename T>
		void parallelize(unsigned long int id, std::vector<T> * data, size_t size){
			int sizePerProc = size/ (comm->numProcs - 1);
			size_t offset = 0;

			for (int i = 1; i < comm->numProcs; ++i){
				int dataPerProc = size/ (comm->numProcs - 1);
				int rem = size % (comm->numProcs -1);
				if (i <= rem)
					dataPerProc += 1;
				std::cerr << "    S:FDDSetData P" << i << " " << id << " " << sizePerProc << "B";

				comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
				offset += dataPerProc;
				std::cerr << ".\n";
			}
		}


		void destroyFDD(unsigned long int id);

};



#endif
