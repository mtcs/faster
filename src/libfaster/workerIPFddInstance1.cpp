#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"


template class workerIFdd<int, char *>;
template class workerIFdd<int, int *>;
template class workerIFdd<int, long int *>;
template class workerIFdd<int, float *>;
template class workerIFdd<int, double *>;

