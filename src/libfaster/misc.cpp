#include "misc.h"

fddType decodeType(size_t code){
	if ( code == typeid(char).hash_code() )
		return Char;
	if ( code == typeid(int).hash_code() )
		return Int;
	if ( code == typeid(long int).hash_code())
		return LongInt;
	if ( code == typeid(float).hash_code())
		return Float;
	if ( code == typeid(double).hash_code())
		return Double;
	if ( code == typeid(void).hash_code())
		return Custom;
	if ( code == typeid(std::string).hash_code())
		return String;
	if ( code == typeid(std::vector<char>).hash_code() )
		return CharV;
	if ( code == typeid(std::vector<int>).hash_code() )
		return IntV;
	if ( code == typeid(std::vector<long int>).hash_code())
		return LongIntV;
	if ( code == typeid(std::vector<float>).hash_code())
		return FloatV;
	if ( code == typeid(std::vector<double>).hash_code())
		return DoubleV;
	/*if ( code == typeid(int *).hash_code() )
		return IntP;
	if ( code == typeid(long int *).hash_code())
		return LongIntP;
	if ( code == typeid(float *).hash_code())
		return FloatP;
	if ( code == typeid(double *).hash_code())
		return DoubleP; // */
	if ( code == typeid(void *).hash_code())
		return Custom;
	return Null;
}
