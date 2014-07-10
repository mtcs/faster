#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<std::string, std::vector<char>>;
template class workerIFdd<std::string, std::vector<int>>;
template class workerIFdd<std::string, std::vector<long int>>;
template class workerIFdd<std::string, std::vector<float>>;
template class workerIFdd<std::string, std::vector<double>>;

