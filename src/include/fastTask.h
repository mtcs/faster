#ifndef LIBFASTER_FASTTASK_H
#define LIBFASTER_FASTTASK_H


#include <vector>
#include "definitions.h"
#include "misc.h"


namespace faster{
	class fastTask{
		public:
			unsigned long int id;
			unsigned long int srcFDD;
			unsigned long int destFDD;
			fddOpType	operationType;
			int		functionId;
			size_t size;
			void * result;
			size_t resultSize;
			size_t workersFinished;
			std::vector<size_t> times;
			size_t duration;
			double * allocation;
			std::vector<procstat> procstats;
			std::vector< std::tuple<void*, size_t, int> > globals;

	};
}

#endif
