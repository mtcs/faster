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
		void * data = NULL;
		void * keys = NULL;
		//void ** data2D = NULL;
		size_t * lineSizes = NULL;
		size_t size, offset;
		std::string name;

		// Wait for a message to arrive
		comm->probeMsgs(tag);

		switch(tag){
			case MSG_TASK:
				std::cerr << "    R:Task ";
				comm->recvTask(task);
				std::cerr << "ID:" << task.id << " FDD:" << task.srcFDD  << " F:" << task.functionId << " ";
				solve(task); // TODO Separate in a different thread ?
				std::cerr << ".\n";
				break;

			case MSG_CREATEFDD:
				std::cerr << "    R:CreateFdd";
				comm->recvCreateFDD(id, tType, size);
				std::cerr << " ID:" << id << " T:" << (int) tType << " S:" << size << " ";
				createFDD(id, tType, size);
				std::cerr << ".\n";
				break;

			case MSG_CREATEIFDD:
				std::cerr << "    R:CreateIFdd ";
				comm->recvCreateIFDD(id, kType, tType, size);
				std::cerr << "ID:" << id << " K:" << (int) kType << " T:" << (int) tType << " S:" << size << " ";
				createIFDD(id, kType, tType, size);
				std::cerr << ".\n";
				break;

			case MSG_DESTROYFDD:
				std::cerr << "    R:DestroyFdd ";
				comm->recvDestroyFDD(id);
				std::cerr << "ID:" << id << " ";
				destroyFDD(id);
				std::cerr << ".\n";
				break;

			case MSG_FDDSETDATAID:
				std::cerr << "    R:SetFddData ";
				comm->recvFDDSetData(id, data, size);
				std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDData(id, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FDDSET2DDATAID:
				std::cerr << "    R:SetFdd2DData ";
				//comm->recvFDDSetData(id, data2D, lineSizes, size);
				comm->recvFDDSetData(id, data, lineSizes, size);
				std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDData(id, data, lineSizes, size);
				//delete [] data2D;
				std::cerr << ".\n";
				break;

			case MSG_FDDSETIDATAID:
				std::cerr << "    R:SetFddIData ";
				comm->recvFDDSetIData(id, keys, data, size);
				std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDIData(id, keys, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FDDSET2DIDATAID:
				std::cerr << "    R:SetFdd2DIData ";
				comm->recvFDDSetIData(id, keys, data, lineSizes, size);
				std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDIData(id, keys, data, lineSizes, size);
				//delete [] data2D;
				std::cerr << ".\n";
				break;

			case MSG_READFDDFILE:
				std::cerr << "    R:ReadFddFile " ;
				comm->recvReadFDDFile(id, name, size, offset);
				std::cerr << "ID:" << id <<" F:" << name<< "(offset:" << offset << ")";
				readFDDFile(id, name, size, offset);
				std::cerr << ".\n";
				break;

			case MSG_COLLECT:
				std::cerr << "    R:Collect ";
				comm->recvCollect(id);
				std::cerr << "ID:" << id << " ";
				//getFDDData(id, data, size);
				//comm->sendFDDData(id, 0, data, size);
				collect(id);
				std::cerr << ".\n";
				break;

			case MSG_FINISH:
				std::cerr << "    R:FINISH ";
				comm->recvFinish();
				finished = true;
				std::cerr << ".\n";
				break;
			default:
				std::cerr << "    R:ERROR UNRECOGNIZED MESSAGE!!!!! ";
				break;
		}
	}
	std::cerr << "  DONE" << '\n';
}


