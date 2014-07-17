#include "workerFdd.h"

extern template class workerFdd<char>;
extern template class workerFdd<int>;
extern template class workerFdd<long int>;
extern template class workerFdd<float>;
extern template class workerFdd<double>;
 
extern template class workerFdd<char*>;
extern template class workerFdd<int*>;
extern template class workerFdd<long int*>;
extern template class workerFdd<float*>;
extern template class workerFdd<double*>;
 
extern template class workerFdd<std::string>;
 
extern template class workerFdd<std::vector<char>>;
extern template class workerFdd<std::vector<int>>;
extern template class workerFdd<std::vector<long int>>;
extern template class workerFdd<std::vector<float>>;
extern template class workerFdd<std::vector<double>>;
