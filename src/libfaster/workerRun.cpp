#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "fastComm.h"
#include "worker.h"

void faster::worker::run(){
	//std::cerr << "  Worker Working..." << '\n';
	while (! finished ){
		int tag;
		int msgSrc;
		fastTask task;
		unsigned long int id;
		fddType tType, kType;
		void * data = NULL;
		void * keys = NULL;
		//void ** data2D = NULL;
		size_t * lineSizes = NULL;
		size_t size, offset;
		std::string name, name2;
		std::vector<unsigned long int> idV;

		// Wait for a message to arrive
		comm->probeMsgs(tag, msgSrc);

		switch(tag){
			case MSG_TASK:
				//std::cerr << "    R:Task \n";
				comm->recvTask(task);
				//std::cerr << "ID:" << task.id << " FDD:" << task.srcFDD  << " F:" << task.functionId << " ";
				solve(task);
				//std::cerr << ".\n";
				break;

			case MSG_CREATEFDD:
				//std::cerr << "    R:CreateFdd\n";
				comm->recvCreateFDD(id, tType, size);
				//std::cerr << " ID:" << id << " T:" << (int) tType << " S:" << size << " ";
				createFDD(id, tType, size);
				//std::cerr << ".\n";
				break;

			case MSG_CREATEIFDD:
				//std::cerr << "    R:CreateIFdd \n";
				comm->recvCreateIFDD(id, kType, tType, size);
				//std::cerr << "ID:" << id << " K:" << (int) kType << " T:" << (int) tType << " S:" << size << "\n";
				createIFDD(id, kType, tType, size);
				//std::cerr << ".\n";
				break;
			case MSG_CREATEGFDD:
				//std::cerr << "    R:CreateFDDGroup \n";
				comm->recvCreateFDDGroup(id, kType, idV);
				//std::cerr << "ID:" << id << " K:" << (int) kType << " NumMembers:" << (int) idV.size() << " ";
				createFDDGroup(id, kType, idV );
				//std::cerr << ".\n";
				break;

			case MSG_DISCARDFDD:
				//std::cerr << "    R:DestroyFdd\n";
				comm->recvDiscardFDD(id);
				//std::cerr << "ID:" << id << " ";
				discardFDD(id);
				//std::cerr << ".\n";
				break;

			case MSG_FDDSETDATAID:
				//std::cerr << "    R:SetFddData\n";
				comm->recvFDDSetData(id, data, size);
				//std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDData(id, data, size);
				//std::cerr << ".\n";
				break;

			case MSG_FDDSET2DDATAID:
				//std::cerr << "    R:SetFdd2DData ";
				//comm->recvFDDSetData(id, data2D, lineSizes, size);
				comm->recvFDDSetData(id, data, lineSizes, size);
				//std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDData(id, data, lineSizes, size);
				//delete [] data2D;
				//std::cerr << ".\n";
				break;

			case MSG_FDDSETIDATAID:
				//std::cerr << "    R:SetFddIData \n";
				comm->recvFDDSetIData(id, keys, data, size);
				//std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDIData(id, keys, data, size);
				//std::cerr << ".\n";
				break;

			case MSG_FDDSET2DIDATAID:
				//std::cerr << "    R:SetFdd2DIData ";
				comm->recvFDDSetIData(id, keys, data, lineSizes, size);
				//std::cerr << "ID:" << id << " S:" << size << " ";
				setFDDIData(id, keys, data, lineSizes, size);
				//delete [] data2D;
				//std::cerr << ".\n";
				break;

			case MSG_READFDDFILE:
				//std::cerr << "    R:ReadFddFile \n" ;
				comm->recvReadFDDFile(id, name, size, offset);
				//std::cerr << "ID:" << id <<" F:" << name << "(offset:" << offset << ")";
				createFDDFromFile(id, name, size, offset);
				//std::cerr << ".\n";
				break;
			case MSG_WRITEFDDFILE:
				//std::cerr << "    R:ReadFddFile \n" ;
				comm->recvWriteFDDFile(id, name, name2);
				//std::cerr << "ID:" << id <<" F:" << name << "(offset:" << offset << ")";
				writeFDDFile(id, name, name2);
				//std::cerr << ".\n";
				break;

			case MSG_COLLECT:
				//std::cerr << "    R:Collect \n";
				comm->recvCollect(id);
				//std::cerr << "ID:" << id << " ";
				//getFDDData(id, data, size);
				//comm->sendFDDData(id, 0, data, size);
				collect(id);
				//std::cerr << ".\n";
				break;

			case MSG_FINISH:
				//std::cerr << "    R:FINISH \n";
				comm->recvFinish();
				finished = true;
				//std::cerr << ".\n";
				break;
			default:
				std::cerr << "    \033[1;31mR:ERROR UNRECOGNIZED MESSAGE " << tag << " from " << msgSrc << "!!!!!!!!\033[0m\n";
				usleep(100000);
				break;
		}
	}
	//std::cerr << "  Worker " << comm->getProcId() << " DONE" << '\n';
}


