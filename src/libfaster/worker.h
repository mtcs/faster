#ifndef LIBFASTER_WORKER_H
#define LIBFASTER_WORKER_H

#include <list>

class worker;

#include "fastContext.h"
#include "workerFdd.h"


// Worker class
// Responsible for receiving and executing tasks and createing workerFDDs .
class worker{
	friend class fastContext;
	private:
		unsigned long int id;
		std::list< std::pair<void *, fddType> > fddList;
		fastComm * comm;
		char tag;
		bool finished;
		char * buffer;

		worker(fastComm * c, void **& ft);
		~worker();

		void run();

		// Worker functions
		void createFDD (unsigned long int id, fddType type);
		void destroyFDD(unsigned long int id);
		void insertDataFDD(unsigned long int id, void * data, size_t size);
		void setFDDOwnership(unsigned long int id, size_t low, size_t up);
		void readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset);

		void solve(fastTask & task);
};

#endif
