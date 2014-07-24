#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new    _workerIFdd<long int, char>(id, type, size); break;
		case Int: return new     _workerIFdd<long int, int>(id, type, size); break;
		case LongInt: return new _workerIFdd<long int, long int>(id, type, size); break;
		case Float: return new   _workerIFdd<long int, float>(id, type, size); break;
		case Double: return new  _workerIFdd<long int, double>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<long int, char>;
template class faster::_workerIFdd<long int, int>;
template class faster::_workerIFdd<long int, long int>;
template class faster::_workerIFdd<long int, float>;
template class faster::_workerIFdd<long int, double>;


