#include <stdlib.h>
#include "libfaster.h"

int map1(int & input){
	return input * 2;
}

int main(int argc, char ** argv){
	fastContext fc("local");
	
	fdd <int> data(fc);

	fdd <int> * result = data.map(&map1)

	return EXIT_SUCCESS;
}
