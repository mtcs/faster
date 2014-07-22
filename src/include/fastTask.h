#ifndef LIBFASTER_FASTTASK_H
#define LIBFASTER_FASTTASK_H


#include "definitions.h"


namespace faster{
	class fastTask{
		public:
			unsigned long int id;
			unsigned long int srcFDD;
			unsigned long int destFDD;
			fddOpType	operationType;
			unsigned int	functionId;
			void * result;
			size_t resultSize;
			size_t workersFinished;
	};
}

#endif
