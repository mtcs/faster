#include "fastComm.h"
#include "worker.h"

worker::worker(fastComm * c, void **& ft){
	comm = c;
	finished = false;
}

worker::~worker(){
}

void worker::run(){
	while (! finished ){
		int tag;
		fastTask task;
		unsigned long int id;
		fddType type;
		void * data;
		size_t size, l, u, offset;
		std::string name;

		// Wait for a message to arrive
		comm->probeMsgs(tag);

		switch(tag){
			case MSG_TASK:
				comm->recvTask(task);
				solve(task); // TODO Separate in a different thread ?
				comm->sendTaskResult(task.id, NULL);
				break;

			case MSG_CREATEFDD:
				comm->recvCreateFDD(id, type);
				createFDD(id, type);
				break;

			case MSG_DESTROYFDD:
				comm->recvDestroyFDD(id);
				destroyFDD(id);
				break;

			case MSG_FDDDATAID:
				comm->recvFDDData(id, data, size);
				insertDataFDD(id, data, size);
				break;

			case MSG_FDDDATAOWN:
				comm->recvFDDDataOwn(id, l, u);
				setFDDOwnership(id, l, u);
				break;

			case MSG_READFDDFILE:
				comm->recvReadFDDFile(id, name, size, offset);
				readFDDFile(id, name, size, offset);
				break;

			case MSG_FINISH:
				comm->recvFinish();
				finished = true;
				break;
			default:
				break;
		}
	}
}
