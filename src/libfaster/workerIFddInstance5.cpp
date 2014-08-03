#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new    _workerIFdd<std::string, char>(id, String, type, size); break;
		case Int: return new     _workerIFdd<std::string, int>(id, String, type, size); break;
		case LongInt: return new _workerIFdd<std::string, long int>(id, String, type, size); break;
		case Float: return new   _workerIFdd<std::string, float>(id, String, type, size); break;
		case Double: return new  _workerIFdd<std::string, double>(id, String, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<std::string, char>;
template class faster::_workerIFdd<std::string, int>;
template class faster::_workerIFdd<std::string, long int>;
template class faster::_workerIFdd<std::string, float>;
template class faster::_workerIFdd<std::string, double>;



