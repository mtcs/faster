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

		std::queue<fastTask> taskQueue;

		unsigned long int createFDD(size_t typeCode);
		unsigned long int readFDD(const char * fileName);

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);

		template <typename T> T collectTaskResult(unsigned long int taskId){
			size_t rSize;
			void * result;
			double time;
			for (int i = 1; i < comm->numProcs; ++i){
				comm->recvTaskResult( id, result, rSize, time );
				std::cerr << "    R:TaskResult" << result << '\n';
			}
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
				comm->sendFDDSetData(id, i, &data[(i - 1) * sizePerProc], sizePerProc);
				std::cerr << "    S:FDDSetData" << i << sizePerProc <<'\n';
			}
			comm->waitForReq(comm->numProcs - 1);
		}

		void destroyFDD(unsigned long int id);

};

#endif
