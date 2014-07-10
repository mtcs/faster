#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"



template class workerIFdd<int, std::vector<char>>;
template class workerIFdd<int, std::vector<int>>;
template class workerIFdd<int, std::vector<long int>>;
template class workerIFdd<int, std::vector<float>>;
template class workerIFdd<int, std::vector<double>>;
