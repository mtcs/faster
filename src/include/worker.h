#ifndef LIBFASTER_WORKER_H
#define LIBFASTER_WORKER_H

#include <vector>
#include <string>

class worker;
class fastContext;
class fastComm;

#include "workerFddBase.h"
#include "fastTask.h"


// Worker class
// Responsible for receiving and executing tasks and createing workerFDDs .
class worker{
	friend class fastContext;
	private:
		unsigned long int id;
		std::vector< workerFddBase * > fddList;
		fastComm * comm;
		char tag;
		bool finished;
		char * buffer;
		void ** funcTable;

		worker(fastComm * c, void ** ft);
		~worker();

		void run();

		// Worker functions
		void createFDD (unsigned long int id, fddType type, size_t size);
		template <typename K>
		void _createIFDD (unsigned long int id, fddType type, size_t size);
		void createIFDD (unsigned long int id, fddType kType, fddType tType, size_t size);
		void destroyFDD(unsigned long int id);
		void setFDDData(unsigned long int id, void * data, size_t size);
		void setFDDData(unsigned long int id, void ** data, size_t * lineSizes, size_t size);
		void getFDDData(unsigned long int id, void *& data, size_t &size);
		void setFDDOwnership(unsigned long int id, size_t low, size_t up);
		void readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset);

		void preapply(fastTask &task, workerFddBase * destFDD);
		
		void solve(fastTask & task);
};

#endif
