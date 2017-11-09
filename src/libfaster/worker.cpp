#include <string>
#include <fstream>
#include <iostream>
#include <chrono>

#include "fastComm.h"
#include "fastCommBuffer.h"
#include "workerFdd.h"
#include "worker.h"

faster::worker::worker(fastComm * c, void ** ft, std::vector< std::tuple<void*, size_t, int> > & globalTable){
	//std::cerr << "  Starting Worker " << c->getProcId() << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
	fddList.reserve(1024);
	this->globalTable = &globalTable;
}

faster::worker::~worker(){
}


void faster::worker::discardFDD(unsigned long int id){
	delete fddList[id];
	fddList[id] = NULL;
}

void faster::worker::setFDDData(unsigned long int id, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( data, size );
}
void faster::worker::setFDDIData(unsigned long int id, void * keys, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( keys, data, size );
}

void faster::worker::setFDDData(unsigned long int id, void * data, size_t * lineSizes, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( data, lineSizes, size );
}

void faster::worker::setFDDIData(unsigned long int id, void * keys, void * data, size_t * lineSizes, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( keys, data, lineSizes, size );
}

/*void faster::worker::getFDDData(unsigned long int id, void *& data, size_t &size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	data = fdd->getData();
	size = fdd->getSize();
}*/


void faster::worker::calibrate(){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	fastCommBuffer &buffer = comm->getResultBuffer();
	const int TESTVECSIZE = 1000000;
	char ret = 0;

	buffer.reset();
	buffer << size_t(0);

	auto start = system_clock::now();
	auto end = system_clock::now();

	#pragma omp parallel
	{
		std::vector<double> v(TESTVECSIZE, 0);
		double a = 0;


		#pragma omp master
		start = system_clock::now();

		#pragma omp barrier

		#pragma omp for schedule(static,100)
		for ( size_t i = 0; i < 4000000; ++i){
			for ( size_t j = 1; j < 5; ++j){
				a += i + j;
				a -= i - j;
				a *= i * j;
				a /= 1 + i / j;
			}
			a += v[ size_t( i * 20011 ) % TESTVECSIZE  ];
			v [ size_t( i * 4099 ) % TESTVECSIZE ] = a;
		}

		#pragma omp master
		end = system_clock::now();
	}
	auto duration = duration_cast<milliseconds>(end - start);

	buffer << size_t(duration.count());
	buffer << getProcStat();
	buffer << size_t(1);
	buffer << ret;

	comm->sendTaskResult();
}

void faster::worker::writeFDDFile(unsigned long int id, std::string &path, std::string &sufix){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	char ret = 0;
	fastCommBuffer &buffer = comm->getResultBuffer();
	workerFddBase * src = fddList[id];

	buffer.reset();
	buffer << size_t(0);

	auto start = system_clock::now();
	auto end = system_clock::now();

	src->writeToFile(&path, comm->getProcId(), &sufix);

	auto duration = duration_cast<milliseconds>(end - start);

	buffer << size_t(duration.count());
	buffer << getProcStat();
	buffer << size_t(1);
	buffer << ret;

	comm->sendTaskResult();
}

//void resizeVector(std::tuple<void *, size_t, int> & var UNUSED){
//}

template <class K>
void readUmapStep2(void * src, size_t bsize, void * dest, int type){
	faster::fastCommBuffer b(0);
	b.setBuffer(src, bsize);
	b.reset();
	//std::cerr << "RBuffer:";
	//for ( size_t i = 0 ; i < b.size() ; i++ ){
		//std::cerr << std::hex << (short) b.data()[i] << " ";
	//}
	//std::cerr << "\n";
	switch (type){
		case Int:
			//std::cerr << "DESERIALIZING UMAP<K,int>\n";
			((std::unordered_map<K, int> *) dest) -> clear();
			b >> *(std::unordered_map<K, int> *) dest;
			break;
		case Double:
			//std::cerr << "DESERIALIZING UMAP<K,double>" << ((std::unordered_map<K, double> *) dest)->size() << "\n";
			((std::unordered_map<K, double> *) dest)->clear();
			b >> *(std::unordered_map<K, double> *) dest;
			//std::cerr << "DESERIALIZATION DONE\n";
			break;
		default:
			std::cerr << "ERROR unordered_map value type("<<type<<") not identified\n";
	}
}
void readUmap(void * src, size_t bsize, void * dest, int type){
	int ktype = (type >> 8) & 0xFF;
	int ttype = type & 0xFF;
	switch (ktype){
		case Int:
			//std::cerr << "DESERIALIZING UMAP<int,?>\n";
			readUmapStep2<int>(src, bsize, dest, ttype);
			break;
		case String:
			//std::cerr << "DESERIALIZING UMAP<string,?>\n";
			readUmapStep2<std::string>(src, bsize, dest, ttype);
			break;
		default:
			std::cerr << "ERROR: unordered_map key type("<<ktype<<") not identified\n";
	}
}

void faster::worker::updateGlobals(fastTask &task){
	for ( size_t i = 0; i < task.globals.size(); i++){
		void * data = std::get<0>(task.globals[i]);
		size_t s = std::get<1>(task.globals[i]);
		int type = std::get<2>(task.globals[i]);

		//if (type & VECTOR){
		//	resizeVector((*globalTable)[i]);
		//}
		//std::cerr << "Update Glob.: "<< i <<"T:" << type << " S:" << s << "\n";
		//std::cerr << "\033[1;33mVar Buffer:";
		//for ( size_t i = 0 ; i < s ; i++ ){
		//	std::cerr << std::hex << (short) ((char*)data)[i] << " ";
		//}
		//std::cerr << "\033[0m\n";
		if (type & POINTER){
			char ** v = (char **) &std::get<0>((*globalTable)[i]);
			if (*v == NULL)
				(*v) = new char[s];
			std::memcpy(
				*v,
				data,
				s
				);
		}else{
			if (type & UMAP){
				readUmap(
					data,
					s,
					std::get<0>((*globalTable)[i]),
					type);
			}else{
				std::memcpy(
					std::get<0>((*globalTable)[i]),
					data,
					s
					);
			}
		}

		delete [] (char*) std::get<0>(task.globals[i]);
	}
}

void faster::worker::solve(fastTask &task){
	updateGlobals(task);

	if (task.operationType == OP_Calibrate){
		calibrate();
		return;
	}

	workerFddBase * src = fddList[task.srcFDD];
	workerFddBase * dest = NULL;

	if (src == NULL) { std::cerr << "\nERROR: Could not find FDD " << task.srcFDD << " !"; exit(201); }

	if ( task.operationType & OP_GENERICMAP)
		dest = fddList[task.destFDD];

	if ( task.functionId != -1  )
		src->preapply(task.id, funcTable[task.functionId], task.operationType, dest, comm);
	else
		src->preapply(task.id, NULL, task.operationType, dest, comm);
}

void faster::worker::collect(unsigned long int id){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD " << id << " !"; exit(202); }

	fdd->collect(comm);
}
