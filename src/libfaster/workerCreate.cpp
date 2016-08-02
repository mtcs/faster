#include <iostream>
#include <fstream>

#include "fastComm.h"
#include "workerFdd.h"
#include "worker.h"

void faster::worker::createFDD (unsigned long int id, fddType type, size_t size){
	//std::cerr << "createFDD ";
	workerFddBase * newFdd;
	newFdd = new workerFdd(id, type, size);
	fddList.insert(fddList.end(), newFdd);
}


void faster::worker::readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset){
	std::string line;
	char c;

	//workerFdd<std::string> * newFdd = new workerFdd<std::string>(id, String);
	workerFdd * newFdd = new workerFdd(id, String);

	if (newFdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fddList.insert(fddList.end(), newFdd);

	// TODO Treat other kinds of input files
	std::ifstream inFile(filename, std::ifstream::in);

	if ( ! inFile.good() ){
		std::cerr << "\nERROR: Could not read input File " << filename << "\n";
		exit(202);
	}


	if( offset > 0){
		inFile.seekg(offset-1, inFile.beg);
		c = inFile.get();
		// If the other process doesn't have this line, get it!
		std::getline( inFile, line );
		if ( c == '\n' ) {
			//std::cerr << "(" << line << ")\n";
			newFdd->insert(0, &line, 0);
		}
	}

	// Start reading lines
	while( size_t(inFile.tellg()) < (offset + size) ){
		std::getline( inFile, line );

		//std::cerr << line << "\n";
		//std::cerr << "[" << line << "]\n";

		newFdd->insert(0, &line, 0);
	}
	inFile.close();

	newFdd->shrink();

	//std::cerr << "    S:FDDInfo ";
	comm->sendFDDInfo(newFdd->getSize());

}


