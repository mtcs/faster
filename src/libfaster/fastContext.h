#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <list>
#include <queue>
#include <typeinfo>


#include "fastComm.h"
#include "fddBase.h"

class fastSettings{
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
		std::list< fddBase * > fddList;
		void ** funcTable;
		fastComm * comm;

		std::queue<fastTask> taskQueue;

		unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId);

		template <typename T> T collectTaskResult(unsigned long int tid){
			comm->waitForResult<T>(tid);
		}

		unsigned long int createFDD(char);
		unsigned long int readFDD(const char * fileName);

		template <typename T> T * collectRDD(unsigned long int id){
		}

		// Propagate FDD data to other machines
		template <typename T>
		void parallelize(T * data, size_t size, unsigned long int id ){
			comm->sendFDDData(id, data, size);
		}

		void destroyFDD(unsigned long int id);

};

#endif
