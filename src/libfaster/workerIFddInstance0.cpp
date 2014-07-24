#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new    _workerIFdd<char, char>(id, type, size); break;
		case Int: return new     _workerIFdd<char, int>(id, type, size); break;
		case LongInt: return new _workerIFdd<char, long int>(id, type, size); break;
		case Float: return new   _workerIFdd<char, float>(id, type, size); break;
		case Double: return new  _workerIFdd<char, double>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<char, char>;
template class faster::_workerIFdd<char, int>;
template class faster::_workerIFdd<char, long int>;
template class faster::_workerIFdd<char, float>;
template class faster::_workerIFdd<char, double>;



