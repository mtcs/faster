#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<long int, std::vector<char>>;
template class workerIFdd<long int, std::vector<int>>;
template class workerIFdd<long int, std::vector<long int>>;
template class workerIFdd<long int, std::vector<float>>;
template class workerIFdd<long int, std::vector<double>>;
