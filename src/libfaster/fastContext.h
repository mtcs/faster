#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <vector>
#include <queue>
#include <typeinfo>
#include <math.h>


#include "fastComm.h"
#include "fddBase.h"

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
		fastContext( std::string m, void ** ft): fastContext(fastSettings(m), ft){}
		fastContext( const fastSettings & s, void **& ft);
		~fastContext();

	private:
		int id;
		unsigned long int numFDDs;
		unsigned long int numTasks;
		fastSettings * settings;
		//std::list< std::pair<void *, fddType> > fddList;
		std::vector< fddBase * > fddList;
		void ** funcTable;
		fastComm * comm;

		std::vector<fastTask *> taskList;

		unsigned long int createFDD(fddBase * ref, size_t typeCode);
		unsigned long int createFDD(fddBase * ref, size_t typeCode, size_t size);
		unsigned long int readFDD(fddBase * ref, const char * fileName);
		void getFDDInfo(size_t & size);
		int numProcs(){ return comm->numProcs; }
		

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);

		void recvTaskResult(unsigned long int &id, void * result, size_t & size){
				double time;

				std::cerr << "    R:TaskResult" << id << '\n';
				comm->recvTaskResult(id, result, size, time);

				taskList[id]->workersFinished++;
		}


		template <typename T> T * collectRDD(unsigned long int id, size_t s){
			T * data = new T[s];
			T * rdata ;
			size_t recvData = 0, rsize;
			unsigned long int rid;

			comm->sendCollect(id);

			while (recvData < s){// TODO put a timeout here
				comm->recvFDDData(rid, &data[recvData], rsize);
				recvData += rsize;
			}

		}

		// Propagate FDD data to other machines
		template <typename T>
		void parallelize(T * data, size_t size, unsigned long int id ){
			//int numBlocks = ceil ( size / settings->blockSize );
			//int blocksPerProc = numBlocks / (comm->numProcs - 1); // TODO DYNAMICALLY VARIATE BLOCK PER PROC LATER
			int sizePerProc = size/ (comm->numProcs - 1);

			for (int i = 1; i < comm->numProcs; ++i){
				//comm->sendFDDSetData(id, i, &data[(i - 1) * blocksPerProc * settings->blockSize], size * sizeof(T));
				std::cerr << "    S:FDDSetData P" << i << " " << id << " " << sizePerProc * sizeof(T) << "B";
				comm->sendFDDSetData(id, i, &data[(i - 1) * sizePerProc], sizePerProc * sizeof(T));
				std::cerr << ".\n";
			}
			comm->waitForReq(comm->numProcs - 1);
		}

		void destroyFDD(unsigned long int id);

};

#endif
