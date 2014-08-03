#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case CharP: return new    _workerIFdd<std::string, char*>(id, String, type, size); break;
		case IntP: return new     _workerIFdd<std::string, int*>(id, String, type, size); break;
		case LongIntP: return new _workerIFdd<std::string, long int*>(id, String, type, size); break;
		case FloatP: return new   _workerIFdd<std::string, float*>(id, String, type, size); break;
		case DoubleP: return new  _workerIFdd<std::string, double*>(id, String, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<std::string, char *>;
template class faster::_workerIFdd<std::string, int *>;
template class faster::_workerIFdd<std::string, long int *>;
template class faster::_workerIFdd<std::string, float *>;
template class faster::_workerIFdd<std::string, double *>;
