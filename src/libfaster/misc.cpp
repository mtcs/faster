#include "misc.h"

char detectType(const char * tName){
	if ( std::string(tName) == "int" ){
		return FDD_TYPE_INT;
	}
	return FDD_TYPE_NULL;
}
