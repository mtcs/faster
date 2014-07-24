#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case String: return new   _workerIFdd<long int, std::string>(id, type, size); break;
		case CharV: return new    _workerIFdd<long int, std::vector<char>>(id, type, size); break;
		case IntV: return new     _workerIFdd<long int, std::vector<int>>(id, type, size); break;
		case LongIntV: return new _workerIFdd<long int, std::vector<long int>>(id, type, size); break;
		case FloatV: return new   _workerIFdd<long int, std::vector<float>>(id, type, size); break;
		case DoubleV: return new  _workerIFdd<long int, std::vector<double>>(id, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<long int, std::vector<char>>;
template class faster::_workerIFdd<long int, std::vector<int>>;
template class faster::_workerIFdd<long int, std::vector<long int>>;
template class faster::_workerIFdd<long int, std::vector<float>>;
template class faster::_workerIFdd<long int, std::vector<double>>;
template class faster::_workerIFdd<long int, std::string>;



