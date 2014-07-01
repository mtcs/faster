#include <vector>
#include <algorithm>

#include "libfaster.h"

#define VECSIZE 100

// --------- Test FDD creation
void testCreation(fastContext & fc){
	std::vector<int> d (VECSIZE);

	std::cerr << "Test Simple > Collect ";
	fdd <int> intFDD (fc, d);
	std::vector < int > tv = intFDD.collect();
	if ( d != tv ) 
		std::cerr << "\033[38;5;196mNOT PASSED\033[38;5;196m\n";
	else
		std::cerr << "\033[38;5;29mPASSED\n";

	//Test Pointer > Collect
	//Test String > Collect
	//Test Vector > Collect
	//Test Indexed Simple > Collect
	//Test Indexed Pointer > Collect
	//Test Indexed String > Collect
	//Test Indexed Vector > Collect
	//Test ReadFile > Collect
}


// --------- Test Simple Non indexed transformations
//Test Simple Map> Simple
//Test Simple Map> Pointer
//Test Simple Map> String
//Test Simple Map> Vector
//Test Pointer Map> Simple
//Test Pointer Map> Pointer
//Test Pointer Map> String
//Test Pointer Map> Vector
//Test String Map> Simple
//Test String Map> Pointer
//Test String Map> String
//Test String Map> Vector
//Test Vector Map> Simple
//Test Vector Map> Pointer
//Test Vector Map> String
//Test Vector Map> Vector
//Test Simple Reduce> Simple
//Test Pointer Reduce> Pointer
//Test String Reduce> String
//Test Vector Reduce> Vector


// --------- Test Indexed FDDs creation
//Test Simple Map> Indexed Simple
//Test Simple Map> Indexed Pointer
//Test Simple Map> Indexed String
//Test Simple Map> Indexed Vector
//Test Pointer Map> Indexed Simple
//Test Pointer Map> Indexed Pointer
//Test Pointer Map> Indexed String
//Test Pointer Map> Indexed Vector
//Test String Map> Indexed Simple
//Test String Map> Indexed Pointer
//Test String Map> Indexed String
//Test String Map> Indexed Vector
//Test Vector Map> Indexed Simple
//Test Vector Map> Indexed Pointer
//Test Vector Map> Indexed String
//Test Vector Map> Indexed Vector


// --------- Test Simple Indexed transformations
//Test Indexed Simple Map> Indexed Simple
//Test Indexed Simple Map> Indexed Pointer
//Test Indexed Simple Map> Indexed String
//Test Indexed Simple Map> Indexed Vector
//Test Indexed Pointer Map> Indexed Simple
//Test Indexed Pointer Map> Indexed Pointer
//Test Indexed Pointer Map> Indexed String
//Test Indexed Pointer Map> Indexed Vector
//Test Indexed String Map> Indexed Simple
//Test Indexed String Map> Indexed Pointer
//Test Indexed String Map> Indexed String
//Test Indexed String Map> Indexed Vector
//Test Indexed Vector Map> Indexed Simple
//Test Indexed Vector Map> Indexed Pointer
//Test Indexed Vector Map> Indexed String
//Test Indexed Vector Map> Indexed Vector
//Test Indexed Simple Reduce> Indexed Simple
//Test Indexed Pointer Reduce> Indexed Pointer
//Test Indexed String Reduce> Indexed String
//Test Indexed Vector Reduce> Indexed Vector


// --------- Test Non-Indexed FDDs creation
//Test Indexed Simple Map> Simple
//Test Indexed Simple Map> Pointer
//Test Indexed Simple Map> String
//Test Indexed Simple Map> Vector
//Test Indexed Pointer Map> Simple
//Test Indexed Pointer Map> Pointer
//Test Indexed Pointer Map> String
//Test Indexed Pointer Map> Vector
//Test Indexed String Map> Simple
//Test Indexed String Map> Pointer
//Test Indexed String Map> String
//Test Indexed String Map> Vector
//Test Indexed Vector Map> Simple
//Test Indexed Vector Map> Pointer
//Test Indexed Vector Map> String
//Test Indexed Vector Map> Vector


int main(int argc, char ** argv){
	fastContext fc("local");
	fc.startWorkers();

	testCreation(fc);
}
