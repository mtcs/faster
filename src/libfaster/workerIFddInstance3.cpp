#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new    _workerIFdd<float, char>(id, type, size); break;
		case Int: return new     _workerIFdd<float, int>(id, type, size); break;
		case LongInt: return new _workerIFdd<float, long int>(id, type, size); break;
		case Float: return new   _workerIFdd<float, float>(id, type, size); break;
		case Double: return new  _workerIFdd<float, double>(id, type, size); break;
	}
	return NULL;
}


template class faster::_workerIFdd<float, char>;
template class faster::_workerIFdd<float, int>;
template class faster::_workerIFdd<float, long int>;
template class faster::_workerIFdd<float, float>;
template class faster::_workerIFdd<float, double>;


