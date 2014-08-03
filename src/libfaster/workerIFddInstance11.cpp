#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case String: return new   _workerIFdd<std::string, std::string>(id, String, type, size); break;
		case CharV: return new    _workerIFdd<std::string, std::vector<char>>(id, String, type, size); break;
		case IntV: return new     _workerIFdd<std::string, std::vector<int>>(id, String, type, size); break;
		case LongIntV: return new _workerIFdd<std::string, std::vector<long int>>(id, String, type, size); break;
		case FloatV: return new   _workerIFdd<std::string, std::vector<float>>(id, String, type, size); break;
		case DoubleV: return new  _workerIFdd<std::string, std::vector<double>>(id, String, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<std::string, std::vector<char>>;
template class faster::_workerIFdd<std::string, std::vector<int>>;
template class faster::_workerIFdd<std::string, std::vector<long int>>;
template class faster::_workerIFdd<std::string, std::vector<float>>;
template class faster::_workerIFdd<std::string, std::vector<double>>;
template class faster::_workerIFdd<std::string, std::string>;



