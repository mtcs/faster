#ifndef LIBFASTER_FASTSCHEDULER_H
#define LIBFASTER_FASTSCHEDULER_H

#include <vector>
#include <tuple>
#include <unordered_map>

#include "definitions.h"

namespace faster{
	class fastTask;

	class fastScheduler{
		private:
			unsigned int numProcs;
			unsigned long int numTasks;
			std::vector<fastTask *> taskList;
			std::vector<double> currentWeights;
			bool _dataMigrationNeeded;

			void updateWeights();
			double * getNewAllocation();
		public:
			fastScheduler(unsigned int numProcs);
			~fastScheduler();
			
			fastTask * enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size);
			fastTask * enqueueTask(fddOpType opT, unsigned long int id, size_t size);
			
			void taskProgress(unsigned long int id, unsigned long int pid, size_t time);

			bool dataMigrationNeeded();
			std::vector<std::list< std::pair<int,long int> >> getDataMigrationInfo();
			std::vector<size_t> getAllocation(size_t size);

	};

}

#endif
