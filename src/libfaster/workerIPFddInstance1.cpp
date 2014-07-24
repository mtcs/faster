#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<int, char*>(id, type, size); break;
		case IntP: return new     _workerIFdd<int, int*>(id, type, size); break;
		case LongIntP: return new _workerIFdd<int, long int*>(id, type, size); break;
		case FloatP: return new   _workerIFdd<int, float*>(id, type, size); break;
		case DoubleP: return new  _workerIFdd<int, double*>(id, type, size); break;
	}
	return NULL;
}


template class faster::_workerIFdd<int, char *>;
template class faster::_workerIFdd<int, int *>;
template class faster::_workerIFdd<int, long int *>;
template class faster::_workerIFdd<int, float *>;
template class faster::_workerIFdd<int, double *>;

