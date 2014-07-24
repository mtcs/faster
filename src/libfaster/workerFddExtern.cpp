#include "_workerFdd.h"

extern template class faster::_workerFdd<char>;
extern template class faster::_workerFdd<int>;
extern template class faster::_workerFdd<long int>;
extern template class faster::_workerFdd<float>;
extern template class faster::_workerFdd<double>;
 
extern template class faster::_workerFdd<char*>;
extern template class faster::_workerFdd<int*>;
extern template class faster::_workerFdd<long int*>;
extern template class faster::_workerFdd<float*>;
extern template class faster::_workerFdd<double*>;
 
extern template class faster::_workerFdd<std::string>;
 
extern template class faster::_workerFdd<std::vector<char>>;
extern template class faster::_workerFdd<std::vector<int>>;
extern template class faster::_workerFdd<std::vector<long int>>;
extern template class faster::_workerFdd<std::vector<float>>;
extern template class faster::_workerFdd<std::vector<double>>;
