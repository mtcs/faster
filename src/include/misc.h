#ifndef LIBFASTER_MISC_H
#define LIBFASTER_MISC_H

#define FDD_TYPE_NULL 	0x00
#define FDD_TYPE_INT 	0x01

#include <typeinfo>
#include <string>
#include <math.h>

#include "definitions.h"

namespace faster{

	class procstat{
		public:
			double ram;
			long unsigned utime;
			long unsigned stime;
			procstat(){
				ram = 0;
				utime = 0;
				stime = 0;
			}
	};

	procstat getProcStat();

	fddType decodeType(size_t typeCode);

	const std::string decodeOptype(fddOpType op);
	const std::string decodeOptypeAb(fddOpType op);

	template < typename T >
	double mean(std::vector<T> v){
		T sum = 0;
		for ( size_t i = 0; i < v.size(); ++i)
			sum += v[i];

		return sum/(double)(v.size());
	}

	template < typename T >
	double max(std::vector<T> v){
		T m = v[0];
		for ( size_t i = 1; i < v.size(); ++i)
			m = std::max( m, v[i] );

		return m;
	}

	template < typename T >
	double sum(std::vector<T> v){
		T sum = 0;
		for ( size_t i = 0; i < v.size(); ++i)
			sum += v[i];

		return sum;
	}

	template < typename T >
	double stdDev(std::vector<T> v, double mean){
		double sum = 0;
		for ( size_t i = 0; i < v.size(); ++i)
			sum += pow(v[i] - (double) mean, 2);

		return ( sqrt( sum/(v.size() - 1) ) );
	}
}

#endif
