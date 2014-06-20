#ifndef LIBFASTER_WORKER_H
#define LIBFASTER_WORKER_H

#include <vector>

class worker;

#include "fastContext.h"
#include "workerFdd.h"


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

		worker(fastComm * c, void **& ft);
		~worker();

		void run();

		// Worker functions
		void createFDD (unsigned long int id, fddType type);
		void destroyFDD(unsigned long int id);
		void setFDDData(unsigned long int id, void * data, size_t size);
		void getFDDData(unsigned long int id, void *& data, size_t &size);
		void setFDDOwnership(unsigned long int id, size_t low, size_t up);
		void readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset);

		template <typename T, typename U>
		void apply(fastTask &task, workerFdd<U> * dest, workerFdd<T> * src);
		template <typename T>
		void preapply(fastTask &task, workerFdd<T> * dest);
		void solve(fastTask & task);
};

#endif
