#include "workerFddCore.cpp"

extern template class faster::workerFddCore<char>;
extern template class faster::workerFddCore<int>;
extern template class faster::workerFddCore<long int>;
extern template class faster::workerFddCore<float>;
extern template class faster::workerFddCore<double>;

extern template class faster::workerFddCore<char*>;
extern template class faster::workerFddCore<int*>;
extern template class faster::workerFddCore<long int*>;
extern template class faster::workerFddCore<float*>;
extern template class faster::workerFddCore<double*>;

extern template class faster::workerFddCore<std::string>;

extern template class faster::workerFddCore<std::vector<char>>;
extern template class faster::workerFddCore<std::vector<int>>;
extern template class faster::workerFddCore<std::vector<long int>>;
extern template class faster::workerFddCore<std::vector<float>>;
extern template class faster::workerFddCore<std::vector<double>>;
