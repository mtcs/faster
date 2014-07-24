#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "workerFdd.h"
#include "worker.h"

void faster::worker::createFDD (unsigned long int id, fddType type, size_t size){
	workerFddBase * newFdd;
	/*switch (type){
		case Null: break;
		case Char: newFdd = new workerFdd<char>(id, type, size); break;
		case Int: newFdd = new workerFdd<int>(id, type, size); break;
		case LongInt: newFdd = new workerFdd<long int>(id, type, size); break;
		case Float: newFdd = new workerFdd<float>(id, type, size); break;
		case Double: newFdd = new workerFdd<double>(id, type, size); break;
		case CharP: newFdd = new workerFdd<char *>(id, type, size); break;
		case IntP: newFdd = new workerFdd<int *>(id, type, size); break;
		case LongIntP: newFdd = new workerFdd<long int *>(id, type, size); break;
		case FloatP: newFdd = new workerFdd<float *>(id, type, size); break;
		case DoubleP: newFdd = new workerFdd<double *>(id, type, size); break;
		//case Custom: newFdd = new workerFdd<void *>(id, type, size); break;
		case String: newFdd = new workerFdd<std::string>(id, type, size); break;
		case CharV: newFdd = new workerFdd<std::vector<char>>(id, type, size); break;
		case IntV: newFdd = new workerFdd<std::vector<int>>(id, type, size); break;
		case LongIntV: newFdd = new workerFdd<std::vector<long int>>(id, type, size); break;
		case FloatV: newFdd = new workerFdd<std::vector<float>>(id, type, size); break;
		case DoubleV: newFdd = new workerFdd<std::vector<double>>(id, type, size); break;
	}// */
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
		if ( c == '\n' ) {
			std::getline( inFile, line ); 
			newFdd->insert(0, &line, 0);
		}
	}
	
	// Start reading lines
	while( size_t(inFile.tellg()) < (offset + size) ){
		std::getline( inFile, line ); 

		newFdd->insert(0, &line, 0);
	}
	inFile.close();

	newFdd->shrink();

	std::cerr << "    S:FDDInfo ";
	comm->sendFDDInfo(newFdd->getSize());

}


