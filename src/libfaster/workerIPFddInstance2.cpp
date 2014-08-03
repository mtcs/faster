#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"



faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<long int, char*>(id, LongInt, type, size); break;
		case IntP: return new     _workerIFdd<long int, int*>(id, LongInt, type, size); break;
		case LongIntP: return new _workerIFdd<long int, long int*>(id, LongInt, type, size); break;
		case FloatP: return new   _workerIFdd<long int, float*>(id, LongInt, type, size); break;
		case DoubleP: return new  _workerIFdd<long int, double*>(id, LongInt, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<long int, char *>;
template class faster::_workerIFdd<long int, int *>;
template class faster::_workerIFdd<long int, long int *>;
template class faster::_workerIFdd<long int, float *>;
template class faster::_workerIFdd<long int, double *>;


