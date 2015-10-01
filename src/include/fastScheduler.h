#ifndef LIBFASTER_FASTSCHEDULER_H
#define LIBFASTER_FASTSCHEDULER_H

#include <vector>
#include <tuple>
#include <unordered_map>

#include "definitions.h"
#include "misc.h"

namespace faster{
	class fastTask;

	class fastScheduler{
		private:
			unsigned int numProcs;
			unsigned long int numTasks;
			std::vector<fastTask *> taskList;
			std::vector<double> currentWeights;
			std::vector<std::string> * funcName;
			bool _dataMigrationNeeded;

			size_t infoPos;

			void updateWeights();
			double * getNewAllocation();
			void resetProcessWeights();
		public:
			fastScheduler(unsigned int numProcs, std::vector<std::string> * funcName);
			~fastScheduler();
			
			fastTask * enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size, std::vector< std::tuple<void*, size_t, int> > & globalTable);
			fastTask * enqueueTask(fddOpType opT, unsigned long int id, size_t size, std::vector< std::tuple<void*, size_t, int> > & globalTable);
			
			void taskProgress(unsigned long int id, unsigned long int pid, size_t time, procstat & stat);
			void taskFinished(unsigned long int id, size_t time);

			void setCalibration(std::vector<size_t> time);
			
			void printProcstats(fastTask * task);
			void printTaskInfo();
			void printTaskInfo(size_t task);
			void printHeader();
			void updateTaskInfo();

			bool dataMigrationNeeded();
			std::vector<std::deque< std::pair<int,long int> >> getDataMigrationInfo();
			std::vector<size_t> getAllocation(size_t size);
			void setAllocation(std::vector<size_t> & alloc, size_t size);

	};

}

#endif
