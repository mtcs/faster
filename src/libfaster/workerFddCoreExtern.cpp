#include "workerFddCore.cpp"

extern template class workerFddCore<char>;
extern template class workerFddCore<int>;
extern template class workerFddCore<long int>;
extern template class workerFddCore<float>;
extern template class workerFddCore<double>;

extern template class workerFddCore<char*>;
extern template class workerFddCore<int*>;
extern template class workerFddCore<long int*>;
extern template class workerFddCore<float*>;
extern template class workerFddCore<double*>;

extern template class workerFddCore<std::string>;

extern template class workerFddCore<std::vector<char>>;
extern template class workerFddCore<std::vector<int>>;
extern template class workerFddCore<std::vector<long int>>;
extern template class workerFddCore<std::vector<float>>;
extern template class workerFddCore<std::vector<double>>;
