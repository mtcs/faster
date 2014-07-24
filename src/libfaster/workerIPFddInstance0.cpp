#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<char, char*>(id, type, size); break;
		case IntP: return new     _workerIFdd<char, int*>(id, type, size); break;
		case LongIntP: return new _workerIFdd<char, long int*>(id, type, size); break;
		case FloatP: return new   _workerIFdd<char, float*>(id, type, size); break;
		case DoubleP: return new  _workerIFdd<char, double*>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<char, char *>;
template class faster::_workerIFdd<char, int *>;
template class faster::_workerIFdd<char, long int *>;
template class faster::_workerIFdd<char, float *>;
template class faster::_workerIFdd<char, double *>;


