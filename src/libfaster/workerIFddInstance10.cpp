#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<double, std::vector<char>>;
template class workerIFdd<double, std::vector<int>>;
template class workerIFdd<double, std::vector<long int>>;
template class workerIFdd<double, std::vector<float>>;
template class workerIFdd<double, std::vector<double>>;
