#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<double, char*>(id, type, size); break;
		case IntP: return new     _workerIFdd<double, int*>(id, type, size); break;
		case LongIntP: return new _workerIFdd<double, long int*>(id, type, size); break;
		case FloatP: return new   _workerIFdd<double, float*>(id, type, size); break;
		case DoubleP: return new  _workerIFdd<double, double*>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<double, char *>;
template class faster::_workerIFdd<double, int *>;
template class faster::_workerIFdd<double, long int *>;
template class faster::_workerIFdd<double, float *>;
template class faster::_workerIFdd<double, double *>;

