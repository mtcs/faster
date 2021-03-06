#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case String: return new   _workerIFdd<char, std::string>(id, Char, type, size); break;
		case CharV: return new    _workerIFdd<char, std::vector<char>>(id, Char, type, size); break;
		case IntV: return new     _workerIFdd<char, std::vector<int>>(id, Char, type, size); break;
		case LongIntV: return new _workerIFdd<char, std::vector<long int>>(id, Char, type, size); break;
		case FloatV: return new   _workerIFdd<char, std::vector<float>>(id, Char, type, size); break;
		case DoubleV: return new  _workerIFdd<char, std::vector<double>>(id, Char, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<char, std::vector<char>>;
template class faster::_workerIFdd<char, std::vector<int>>;
template class faster::_workerIFdd<char, std::vector<long int>>;
template class faster::_workerIFdd<char, std::vector<float>>;
template class faster::_workerIFdd<char, std::vector<double>>;
template class faster::_workerIFdd<char, std::string>;


