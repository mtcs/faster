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


		template <typename T>
		void parallelize(unsigned long int id, T * data, size_t size);
		template <typename T>
		void parallelize(unsigned long int id, T ** data, size_t * dataSizes, size_t size);
		template <typename T>
		void parallelize(unsigned long int id, std::vector<T> * data, size_t size);
		void parallelize(unsigned long int id, std::string * data, size_t size);

		void destroyFDD(unsigned long int id);

};



#endif
