#include <iostream>
#include "workerFdd.h"
#include "workerFddGroup.h"
#include "worker.h"



void faster::worker::createIFDD(unsigned long int id, fddType kType, fddType tType, size_t size){
	workerFddBase * newFdd = new workerFdd(id, kType, tType, size);
	fddList.insert(fddList.end(), newFdd);
}

void faster::worker::createFDDGroup(unsigned long int id, fddType kType, std::vector<unsigned long int> & idV){
	std::vector<workerFddBase *> members(idV.size(), NULL);

	for ( size_t i = 0; i < members.size(); ++i){
		members[i] = fddList[idV[i]];;
	}

	workerFddBase * newFdd;
	switch(kType){
		case Char:
			newFdd = new workerFddGroup<char>(id, kType, members);
			break;
		case Int:
			newFdd = new workerFddGroup<int>(id, kType, members);
			break;
		case LongInt:
			newFdd = new workerFddGroup<long int>(id, kType, members);
			break;
		case Float:
			newFdd = new workerFddGroup<float>(id, kType, members);
			break;
		case Double:
			newFdd = new workerFddGroup<double>(id, kType, members);
			break;
		case String:
			newFdd = new workerFddGroup<std::string>(id, kType, members);
			break;
		default:
			std::cerr << " Error: could not identify FddGroup Key Type!\n";
			break;
	}
	if (newFdd != NULL)
		fddList.insert(fddList.end(), newFdd);
}


