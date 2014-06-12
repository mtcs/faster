#ifndef LIBFASTER_WORKERFDD_H
#define LIBFASTER_WORKERFDD_H

template <class T>
class workerFdd;

#include "fastContext.h"
#include "fddStorage.h"

// Worker side FDD
template <class T>
class workerFdd{
	private:
		unsigned long int id;

		fddStorage <T> localData;

		// Real blocking processing functions
		void runMap (unsigned long int rddIdRes, unsigned long int rddIdSrc);
};

#endif
