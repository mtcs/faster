#include <iostream>
#include <fstream>

#include "fastComm.h"
#include "workerFdd.h"
#include "worker.h"
#include "hdfsEngine.h"

void faster::worker::createFDD (unsigned long int id, fddType type, size_t size){
	//std::cerr << "createFDD ";
	workerFddBase * newFdd;
	newFdd = new workerFdd(id, type, size);
	fddList.insert(fddList.end(), newFdd);
}


void faster::worker::readHDFSFile(unsigned long int id, std::string &filename, size_t size, size_t offset){
	std::string line;
	char c;
	hdfsEngine fs;

	//workerFdd<std::string> * newFdd = new workerFdd<std::string>(id, String);
	workerFdd * newFdd = new workerFdd(id, String);

	if (newFdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fddList.insert(fddList.end(), newFdd);

	// TODO Treat other kinds of input files
	//std::ifstream inFile(filename, std::ifstream::in);
	hdfsFile inFile = fs.open(filename.substr(6,filename.size()-7), R);

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

void faster::worker::readFile(unsigned long int id, std::string &filename, size_t size, size_t offset){
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
void faster::worker::createFDDFromFile(unsigned long int id, std::string &filename, size_t size, size_t offset){
	std::string fileType = filename.substr(0,7);

	if(fileType.size() > 7){
		if(! fileType.compare("hdfs://")){
			readHDFSFile(id, filename, size, offset);
		}else if(! fileType.compare("file://")){
			readFile(id, filename.substr(6,filename.length()-7), size, offset);
		}else{
			readFile(id, filename, size, offset);
		}
	}else{
		readFile(id, filename, size, offset);
	}

}


