#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/times.h>
#include <omp.h>

#include "misc.h"

faster::procstat faster::getProcStat(){
	std::string dump;
	procstat stat;

	struct tms tms0;

	times(&tms0);

	stat.utime = tms0.tms_utime;
	stat.stime = tms0.tms_stime;

	std::ifstream statmFile("/proc/self/statm", std::ifstream::in);
	if( statmFile.good() ){
		statmFile >> dump;
		statmFile >> stat.ram;
		stat.ram *= (double)sysconf(_SC_PAGESIZE) / (1024 * 1024);
	}
	statmFile.close();

	//std::cerr << "      getProcStat: " << stat.ram << " " << stat.utime << " " << stat.stime << "\n";

	return stat;
}

faster::fddType faster::decodeType(size_t code){
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
	if ( code == typeid(void *).hash_code())
		return Custom;
	return Null;
}


const std::string faster::decodeOptype(fddOpType op){
	if (op & OP_GENERICMAP){
		switch(op){
			case OP_Map: 		return "Map        ";
			case OP_BulkMap: 	return "BulkMap    ";
			case OP_FlatMap: 	return "FlatMap    ";
			case OP_BulkFlatMap: 	return "BulkFlatMap";
			case OP_MapByKey: 	return "MapByKey   ";
			case OP_FlatMapByKey:	return "FlatMapByKey";
		}
	}
	if (op & OP_GENERICREDUCE){
		switch(op){
			case OP_Reduce: 	return "Reduce     ";
			case OP_BulkReduce: 	return "BulkReduce ";
		}
	}
	if (op & OP_GENERICUPDATE){
		switch(op){
			case OP_UpdateByKey: 	return "UpdateByKey";
			case OP_BulkUpdate:	return "BulkUpdate ";
		}
	}
	if (op & OP_GENERICMISC){
		switch(op){
			case OP_CountByKey: 	return "CountByKey ";
			case OP_GroupByKey: 	return "GroupByKey ";
			case OP_CoGroup: 	return "Cogroup    ";
			case OP_Calibrate: 	return "Calibrate  ";
		}
	}
	return "           ";
}
const std::string faster::decodeOptypeAb(fddOpType op){
	if (op & OP_GENERICMAP){
		switch(op){
			case OP_Map: 		return "M  ";
			case OP_BulkMap: 	return "BM ";
			case OP_FlatMap: 	return "FM ";
			case OP_BulkFlatMap: 	return "BFM";
			case OP_MapByKey: 	return "MBK";
			case OP_FlatMapByKey:	return "FMK";
		}
	}
	if (op & OP_GENERICREDUCE){
		switch(op){
			case OP_Reduce: 	return "R  ";
			case OP_BulkReduce: 	return "BR ";
		}
	}
	if (op & OP_GENERICUPDATE){
		switch(op){
			case OP_UpdateByKey: 	return "UBK";
			case OP_BulkUpdate:	return "BU ";
		}
	}
	if (op & OP_GENERICMISC){
		switch(op){
			case OP_CountByKey: 	return "CBK";
			case OP_GroupByKey: 	return "GBK";
			case OP_CoGroup: 	return "CG ";
			case OP_Calibrate: 	return "CL ";
		}
	}
	return "           ";
}
