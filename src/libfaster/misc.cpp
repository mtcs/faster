#include "misc.h"

fddType decodeType(size_t code){
	if ( code == typeid(int).hash_code() )
		return Int;
	if ( code == typeid(long int).hash_code())
		return LongInt;
	if ( code == typeid(float).hash_code())
		return Float;
	if ( code == typeid(double).hash_code())
		return Double;
	if ( code == typeid(std::string).hash_code())
		return String;
	if ( code == typeid(void).hash_code())
		return Custom;
	return Null;
}
