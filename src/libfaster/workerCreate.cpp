#include "workerFdd.h"
#include "worker.h"

void worker::createFDD (unsigned long int id, fddType type, size_t size){
	workerFddBase * newFdd;
	switch (type){
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
		//case Custom: //newFdd = new workerFdd<void *>(id, type, size); break;
		case String: newFdd = new workerFdd<std::string>(id, type, size); break;
		//case CharV: newFdd = new workerIFdd<std::vector<char>>(id, type, size); break;
		//case IntV: newFdd = new workerIFdd<std::vector<int>>(id, type, size); break;
		//case LongIntV: newFdd = new workerIFdd<std::vector<long int>(id, type, size); break;
		//case FloatV: newFdd = new workerIFdd<std::vector<float>>(id, type, size); break;
		//case DoubleV: newFdd = new workerIFdd<std::vector<double>>(id, type, size); break;
	}
	fddList.insert(fddList.end(), newFdd);
}



