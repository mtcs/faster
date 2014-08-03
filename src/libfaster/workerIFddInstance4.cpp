#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new    _workerIFdd<double, char>(id, Double, type, size); break;
		case Int: return new     _workerIFdd<double, int>(id, Double, type, size); break;
		case LongInt: return new _workerIFdd<double, long int>(id, Double, type, size); break;
		case Float: return new   _workerIFdd<double, float>(id, Double, type, size); break;
		case Double: return new  _workerIFdd<double, double>(id, Double, type, size); break;
	}
	return NULL;
}


template class faster::_workerIFdd<double, char>;
template class faster::_workerIFdd<double, int>;
template class faster::_workerIFdd<double, long int>;
template class faster::_workerIFdd<double, float>;
template class faster::_workerIFdd<double, double>;


