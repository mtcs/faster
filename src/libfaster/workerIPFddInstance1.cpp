#include "workerIPFddFlatMap.cpp"

template class workerIFdd<long int, char *>;
template class workerIFdd<long int, int *>;
template class workerIFdd<long int, long int *>;
template class workerIFdd<long int, float *>;
template class workerIFdd<long int, double *>;

template class workerIFdd<float, char *>;
template class workerIFdd<float, int *>;
template class workerIFdd<float, long int *>;
template class workerIFdd<float, float *>;
template class workerIFdd<float, double *>;


