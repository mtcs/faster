#include "workerIFdd.h"
#include "worker.h"


template <typename K>
void worker::_createIFDD (unsigned long int id, fddType type, size_t size){
	workerFddBase * newFdd;
	switch (type){
		case Null: break;
		case Char: newFdd = new workerIFdd<K, char>(id, type, size); break;
		case Int: newFdd = new workerIFdd<K, int>(id, type, size); break;
		case LongInt: newFdd = new workerIFdd<K, long int>(id, type, size); break;
		case Float: newFdd = new workerIFdd<K, float>(id, type, size); break;
		case Double: newFdd = new workerIFdd<K, double>(id, type, size); break;
		case CharP: newFdd = new workerIFdd<K, char *>(id, type, size); break;
		case IntP: newFdd = new workerIFdd<K, int *>(id, type, size); break;
		case LongIntP: newFdd = new workerIFdd<K, long int *>(id, type, size); break;
		case FloatP: newFdd = new workerIFdd<K, float *>(id, type, size); break;
		case DoubleP: newFdd = new workerIFdd<K, double *>(id, type, size); break;
		// case Custom: //newFdd = new workerIFdd<K, void *>(id, type, size); break;
		case String: newFdd = new workerIFdd<K, std::string>(id, type, size); break;
		//case CharV: newFdd = new workerIFdd<K, std::vector<char>>(id, type, size); break;
		//case IntV: newFdd = new workerIFdd<K, std::vector<int>>(id, type, size); break;
		//case LongIntV: newFdd = new workerIFdd<K, std::vector<long int>(id, type, size); break;
		//case FloatV: newFdd = new workerIFdd<K, std::vector<float>>(id, type, size); break;
		//case DoubleV: newFdd = new workerIFdd<K, std::vector<double>>(id, type, size); break;
	}
	fddList.insert(fddList.end(), newFdd);
}

void worker::createIFDD(unsigned long int id, fddType kType, fddType tType, size_t size){
	switch (kType){
		case Null: break;
		case Char:    _createIFDD<char>(id, tType, size); break;
		case Int:     _createIFDD<int>(id, tType, size); break;
		case LongInt: _createIFDD<long int>(id, tType, size); break;
		case Float:   _createIFDD<float>(id, tType, size); break;
		case Double:  _createIFDD<double>(id, tType, size); break;
		case String:  _createIFDD<std::string>(id, tType, size); break;
	}
}

