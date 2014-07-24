#include "_workerFdd.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case Char: return new _workerFdd<char>(id, type, size); break;
		case Int: return new _workerFdd<int>(id, type, size); break;
		case LongInt: return new _workerFdd<long int>(id, type, size); break;
		case Float: return new _workerFdd<float>(id, type, size); break;
		case Double: return new _workerFdd<double>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerFdd<char>;
template class faster::_workerFdd<int>;
template class faster::_workerFdd<long int>;
template class faster::_workerFdd<float>;
template class faster::_workerFdd<double>;



