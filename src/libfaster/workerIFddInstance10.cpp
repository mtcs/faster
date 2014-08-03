#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"
#include "workerFddModule.cpp"

faster::workerFddBase * faster::newWorkerSDL(unsigned long int id, fddType type, size_t size){
	switch (type){
		case String: return new   _workerIFdd<double, std::string>(id, Double, type, size); break;
		case CharV: return new    _workerIFdd<double, std::vector<char>>(id, Double, type, size); break;
		case IntV: return new     _workerIFdd<double, std::vector<int>>(id, Double, type, size); break;
		case LongIntV: return new _workerIFdd<double, std::vector<long int>>(id, Double, type, size); break;
		case FloatV: return new   _workerIFdd<double, std::vector<float>>(id, Double, type, size); break;
		case DoubleV: return new  _workerIFdd<double, std::vector<double>>(id, Double, type, size); break;
	}
	return NULL;
}

template class faster::_workerIFdd<double, std::vector<char>>;
template class faster::_workerIFdd<double, std::vector<int>>;
template class faster::_workerIFdd<double, std::vector<long int>>;
template class faster::_workerIFdd<double, std::vector<float>>;
template class faster::_workerIFdd<double, std::vector<double>>;
template class faster::_workerIFdd<double, std::string>;



