#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case String:  return new _workerIFdd<int, std::string>(id, Int, type, size); break;
		case CharV: return new    _workerIFdd<int, std::vector<char>>(id, Int, type, size); break;
		case IntV: return new     _workerIFdd<int, std::vector<int>>(id, Int, type, size); break;
		case LongIntV: return new _workerIFdd<int, std::vector<long int>>(id, Int, type, size); break;
		case FloatV: return new   _workerIFdd<int, std::vector<float>>(id, Int, type, size); break;
		case DoubleV: return new  _workerIFdd<int, std::vector<double>>(id, Int, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<int, std::vector<char>>;
template class faster::_workerIFdd<int, std::vector<int>>;
template class faster::_workerIFdd<int, std::vector<long int>>;
template class faster::_workerIFdd<int, std::vector<float>>;
template class faster::_workerIFdd<int, std::vector<double>>;
template class faster::_workerIFdd<int, std::string>;



