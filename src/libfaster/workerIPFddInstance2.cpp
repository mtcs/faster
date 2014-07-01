#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"


template class workerIFdd<long int, char *>;
template class workerIFdd<long int, int *>;
template class workerIFdd<long int, long int *>;
template class workerIFdd<long int, float *>;
template class workerIFdd<long int, double *>;


