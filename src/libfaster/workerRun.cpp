#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "worker.h"

void worker::run(){
	std::cerr << "  Worker Working..." << '\n';
	while (! finished ){
		int tag;
		fastTask task;
		unsigned long int id;
		fddType tType, kType;
		void * data;
		void ** data2D;
		size_t * lineSizes;
		size_t size, offset;
		std::string name;

		// Wait for a message to arrive
		comm->probeMsgs(tag);

		switch(tag){
			case MSG_TASK:
				comm->recvTask(task);
				std::cerr << "    R:Task ID:" << task.id << " FDD:" << task.srcFDD  << " F:" << task.functionId << " ";
				solve(task); // TODO Separate in a different thread ?
				std::cerr << ".\n";
				break;

			case MSG_CREATEFDD:
				comm->recvCreateFDD(id, tType, size);
				std::cerr << "    R:CreateFdd ID:" << id << " T:" << (int) tType << " S:" << size << " ";
				createFDD(id, tType, size);
				std::cerr << ".\n";
				break;

			case MSG_CREATEIFDD:
				comm->recvCreateIFDD(id, kType, tType, size);
				std::cerr << "    R:CreateFdd ID:" << id << " K:" << (int) kType << " T:" << (int) tType << " S:" << size << " ";
				createIFDD(id, kType, tType, size);
				std::cerr << ".\n";
				break;

			case MSG_DESTROYFDD:
				comm->recvDestroyFDD(id);
				std::cerr << "    R:DestroyFdd ID:" << id << " ";
				destroyFDD(id);
				std::cerr << ".\n";
				break;

			case MSG_FDDSETDATAID:
				comm->recvFDDSetData(id, data, size);
				std::cerr << "    R:SetFddData ID:" << id << " S:" << size << " ";
				setFDDData(id, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FDDSET2DDATAID:
				comm->recvFDDSetData(id, data2D, lineSizes, size);
				std::cerr << "    R:SetFddData ID:" << id << " S:" << size << " ";
				setFDDData(id, data2D, lineSizes, size);
				std::cerr << ".\n";
				break;

			case MSG_READFDDFILE:
				comm->recvReadFDDFile(id, name, size, offset);
				std::cerr << "    R:ReadFddFile " << id <<" F:" << name<< "(offset:" << offset << ")" ;
				readFDDFile(id, name, size, offset);
				std::cerr << ".\n";
				break;

			case MSG_COLLECT:
				comm->recvCollect(id);
				std::cerr << "    R:Collect ID:" << id << " ";
				getFDDData(id, data, size);
				comm->sendFDDData(id, 0, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FINISH:
				comm->recvFinish();
				std::cerr << "    R:FINISH ";
				finished = true;
				std::cerr << ".\n";
				break;
			default:
				break;
		}
	}
	std::cerr << "  DONE" << '\n';
}


