#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"


faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<float, char*>(id, Float, type, size); break;
		case IntP: return new     _workerIFdd<float, int*>(id, Float, type, size); break;
		case LongIntP: return new _workerIFdd<float, long int*>(id, Float, type, size); break;
		case FloatP: return new   _workerIFdd<float, float*>(id, Float, type, size); break;
		case DoubleP: return new  _workerIFdd<float, double*>(id, Float, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<float, char *>;
template class faster::_workerIFdd<float, int *>;
template class faster::_workerIFdd<float, long int *>;
template class faster::_workerIFdd<float, float *>;
template class faster::_workerIFdd<float, double *>;

