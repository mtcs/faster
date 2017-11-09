#include <vector>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <utility>
#include <stdio.h>
#include <iomanip>

// Get RAM
#include "sys/types.h"
#include "sys/sysinfo.h"

#include "fastScheduler.h"
#include "fastCommBuffer.h"
#include "fastTask.h"
#include "misc.h"

faster::fastScheduler::fastScheduler(unsigned int numProcs, std::vector<std::string> * funcName){
	this->numProcs = numProcs;
	numTasks = 0;
	resetProcessWeights();
	_dataMigrationNeeded = false;
	this->funcName = funcName;
	infoPos = 0;
}
faster::fastScheduler::~fastScheduler(){
	for ( auto it = taskList.begin(); it != taskList.end() ; it++ ){
		for (size_t i = 0; i < (*it)->globals.size(); i++){
			delete [] (char*) std::get<0>((*it)->globals[i]);
		}
		delete (*it);
	}
	taskList.clear();
}

void faster::fastScheduler::resetProcessWeights(){
	currentWeights = std::vector<double>( numProcs, 1/(double)(numProcs-1) );
	currentWeights[0] = 0;
	//currentWeights[1] += currentWeights[2]/2;
	//currentWeights[2] /= 2;
}

bool faster::fastScheduler::dataMigrationNeeded(){
	return ( _dataMigrationNeeded && ( taskList.size() > 1 ) );
}

std::vector<std::deque< std::pair<int,long int> >> faster::fastScheduler::getDataMigrationInfo(){
	std::vector<std::deque< std::pair<int,long int> >> r (numProcs, std::deque<std::pair<int,long int>>());
	if (taskList.size() > 1){
		std::vector<std::pair<int,long int>> delta(numProcs,std::pair<int,size_t>(0,0));
		unsigned int i, j;
		fastTask * thisTask = taskList.back();
		fastTask * lastTask = taskList[taskList.size() - 2];

		// Calculate data delta :D
		delta[0].first = 0;
		delta[0].second = 0;
		for( i = 1; i < numProcs; ++i){
			delta[i].first = i;
			delta[i].second = lastTask->size*(thisTask->allocation.get()[0][i] - lastTask->allocation.get()[0][i]);
		}
		std::sort(delta.begin(), delta.end(), [](std::pair<int,size_t> a, std::pair<int,size_t> b){return a.second < b.second;});

		std::cerr << "      [ Migration info: ";
		for( size_t i = 0; i < numProcs; ++i){
			std::cerr << delta[i].first << ":" << delta[i].second << "  ";
		}
		std::cerr << "]\n";

		int carry = 0;
		i = 0;
		j = numProcs-1;
		while (i < j){
			carry = delta[i].second + delta[j].second;
			// Less then enough
			if (carry < 0){
				// To
				r[delta[i].first].push_back(std::make_pair(delta[j].first, delta[i].second));
				// From
				r[delta[j].first].push_back(std::make_pair(delta[i].first, -delta[i].second));
				i++;
			}else{
				// More then enough
				if (carry > 0){
					// To
					r[delta[i].first].push_back(std::make_pair(delta[j].first, -delta[j].second));
					// From
					r[delta[j].first].push_back(std::make_pair(delta[i].first, delta[j].second));
					j--;
				}else{
					// To
					r[delta[i].first].push_back(std::make_pair(delta[j].first, delta[i].second));
					// From
					r[delta[j].first].push_back(std::make_pair(delta[i].first, -delta[i].second));
					i++;
					j--;
				}
			}
		}
	}
	return r;
}

std::vector<size_t> faster::fastScheduler::getAllocation(size_t size){
	std::vector<size_t> r(numProcs, 0);
	size_t sum = 0;
	for(unsigned int i = 1; i < numProcs; ++i){
		r[i] = size * currentWeights[i];
		sum += r[i];
	}

	if ( sum < size ){
		for(unsigned int i = 1; i < numProcs; ++i){
			if ( i <= (size - sum) )
				r[i] ++;
		}
	}

	return  r;
}
void faster::fastScheduler::setAllocation(std::vector<size_t> & alloc, size_t size){
	for ( size_t  i = 0; i < alloc.size(); i++ ){
		currentWeights[i] = (double) alloc[i] / size;
	}
}

void faster::fastScheduler::updateWeights(){
	unsigned int i;
	if (taskList.size() > 0){
		fastTask * lastTask = taskList.back();
		std::vector<double> rate(numProcs, 0);

		//std::cerr << "      [ Exec.Times: ";
		for( i = 1; i < numProcs; ++i){
			std::cerr << lastTask->times[i] << " ";
			rate[i] = lastTask->allocation.get()[0][i]/(double)lastTask->times[i];
		}
		//std::cerr << " ]\n";

		double powersum = 0;
		for( i = 1; i < numProcs; ++i){
			powersum += rate[i] ;
		}

		if (powersum > 0)
			for( i = 1; i < numProcs; ++i){
				currentWeights[i] =  rate[i] / powersum;
			}
		else
			resetProcessWeights();
		currentWeights[0] = 0;
	}
}


std::shared_ptr <std::vector<double>> faster::fastScheduler::getNewAllocation(){
	std::shared_ptr<std::vector<double>> r;
	_dataMigrationNeeded = false;

	// see if the response times are too disbalanced
	if (taskList.size() > 0) {
		auto &t = taskList.back()->times;
		double m = mean(t);
		double sd = stdDev(t, m);
		//if ( m > 800 )
			//std::cerr << "      [ Av. Exec. Time:" << m << " VC:" << sd/m << " ]";
		if ( (m > 300) && ( sd/m > 1 ) ){
			_dataMigrationNeeded = true;
		}
	}

	if ( (taskList.size() > 0) && ( ! _dataMigrationNeeded) ){
		//r = taskList.back()->allocation;
		r = taskList.back()->allocation;
	}else{

		r = std::make_shared<std::vector<double>>(numProcs);
	}

	if ( (taskList.size() > 0) && ( _dataMigrationNeeded) )
		updateWeights();

	r.get()[0][0] = 0;
	//std::cerr << "      [ Processes Weights: ";
	for( size_t i = 1; i < numProcs; ++i){
		r.get()[0][i] = currentWeights[i];
		//std::cerr << r[i] << " ";
	}
	//std::cerr << "]\n";


	return r;
}

//template <class T>
//int serilizeVar(char *& buffer, T & var, size_t pos, size_t & bsize){
//	size_t s = sizeof(T);
//	if (bsize < s){
//		size_t newSize = 1.1*(bsize+s);
//		buffer = (char*) realloc(buffer, newSize);
//		bsize = newSize;
//	}
//	std::memcpy(&buffer[pos], & var, s);
//	return sizeof(T);
//}
//template <class T>
//int serilizeVar(char *& buffer, const T & var, size_t pos, size_t & bsize){
//	size_t s = sizeof(T);
//	if (bsize < s){
//		size_t newSize = 1.1*(bsize+s);
//		buffer = (char*) realloc(buffer, newSize);
//		bsize = newSize;
//	}
//	std::memcpy(&buffer[pos], & var, s);
//	return sizeof(T);
//}
//template <>
//int serilizeVar(char *& buffer, std::string & var, size_t pos, size_t & bsize){
//	size_t s = var.size();
//	if (bsize < s){
//		size_t newSize = 1.1*(bsize+s);
//		buffer = (char*) realloc(buffer, newSize);
//		bsize = newSize;
//	}
//	std::memcpy(&buffer[pos], var.data(), var.size());
//	return var.size();
//}
//template <class K, class T>
//int serilizeVar(char *& buffer, std::unordered_map<K,T> & var, size_t pos, size_t & bsize){
//	pos += serilizeVar(&buffer[pos], var.size(), pos, bsize);
//	for ( auto & it : var ){
//		pos += serilizeVar(&buffer[pos], it.first, pos, bsize);
//		pos += serilizeVar(&buffer[pos], it.second, pos, bsize);
//	}
//	return pos;
//}

template <class K>
void * serilizeUMapStep2(void * umap, int type, size_t & s){
	size_t bsize = 1024*1024;
	char * buffer = new char[bsize];
	faster::fastCommBuffer b(0);
	b.setBuffer(buffer, bsize);
	b.reset();
	switch (type){
		case Int:
			//std::cerr << "SERIALIZING UMAP<K,int>\n";
			//pos += serilizeVar(buffer, *(std::unordered_map<K, int> *)umap, pos, bsize);
			b.write( *(std::unordered_map<K, int> *) umap );
			break;
		case Double:
			//pos += serilizeVar(buffer, *(std::unordered_map<K, double> *)umap, pos, bsize);
			//std::cerr << "SERIALIZING UMAP<K,double>" << ((std::unordered_map<K, double> *) umap )->size() << "\n";
			//b.write<K,double>( *(std::unordered_map<K, double> *) umap );
			b << *(const std::unordered_map<K, double> *) umap ;
			//std::cerr << "SERIALIZATION DONE (" << b.size()/1024 << ")\n";
			break;
		default:
			std::cerr << "ERROR unordered_map value type("<<type<<") not identified\n";
	}
	//std::cerr << "\033[1;33mBuffer:";
	//for ( size_t i = 0 ; i < b.size() ; i++ ){
		//std::cerr << std::hex << (short) b.data()[i] << " ";
	//}
	//std::cerr << "\033[0m\n";
	s = b.size();
	return b.data();

}

void * serilizeUMapStep1(void * umap, int type, size_t & s){
	int ktype = (type >> 8) & 0xFF;
	int ttype = type & 0xFF;
	switch (ktype){
		case Int:
			//std::cerr << "SERIALIZING UMAP<int,?>\n";
			return serilizeUMapStep2<int>(umap, ttype, s);
		case String:
			//std::cerr << "SERIALIZING UMAP<string,?>\n";
			return serilizeUMapStep2<std::string>(umap, ttype, s);
		default:
			std::cerr << "ERROR: unordered_map key type("<<ktype<<") not identified\n";
			return NULL;
	}
}

void faster::fastScheduler::copyTaskGlobals(fastTask * newTask, std::vector< std::tuple<void*, size_t, int> > & globalTable){
	//std::cerr << "COPY GLOBALS\n";

	for ( size_t i = 0; i < globalTable.size(); i++){
		void * var;
		void * varPointer = std::get<0>(globalTable[i]);
		size_t s = std::get<1>(globalTable[i]);
		int type = std::get<2>(globalTable[i]);
		//std::cerr << "T:" << type << " S:" << s << "\n";

		if (type & POINTER){
			//std::cerr << "POINTER ";
			var = new char[s];
			std::memcpy(var, *(char**)varPointer, s);
		}else{
			//std::cerr << "NOTPOINTER ";
			if (type & UMAP){
				var = serilizeUMapStep1(
						varPointer,
						type,
						s
						); // TODO change how this is done!!!! This  should be done by recursive serialization
			}else{
				var = new char[s];
				std::memcpy(var, varPointer, s);
			}
		}

		newTask->globals.insert(newTask->globals.end(), std::make_tuple(var, s, type) );
	}
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size, std::vector< std::tuple<void*, size_t, int> > & globalTable){
	fastTask * newTask = new fastTask();
	newTask->id = numTasks++;
	newTask->srcFDD = idSrc;
	newTask->destFDD = idRes;
	newTask->size = size;
	newTask->operationType = opT;
	newTask->functionId = funcId;
	newTask->workersFinished = 0;
	newTask->allocation = getNewAllocation();
	newTask->duration = 0;
	newTask->times = std::vector<size_t>(numProcs, 0);
	newTask->procstats = std::vector<procstat>(numProcs);

	copyTaskGlobals(newTask, globalTable);

	taskList.insert(taskList.end(), newTask);

	return newTask;
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int sid, size_t size, std::vector< std::tuple<void*, size_t, int> > & globalTable){
	return enqueueTask(opT, sid, 0, -1, size, globalTable );
}

void faster::fastScheduler::taskProgress(unsigned long int tid, unsigned long int pid, size_t time, procstat & stat){
	taskList[tid]->workersFinished++;
	taskList[tid]->times[pid] = time;
	taskList[tid]->procstats[pid] = stat;
}

void faster::fastScheduler::taskFinished(unsigned long int tid, size_t time){
	taskList[tid]->duration = time;
	taskList[tid]->procstats[0] = getProcStat();
}

void faster::fastScheduler::setCalibration(std::vector<size_t> time){
	size_t sum = 0;

	for ( size_t i = 1; i < time.size(); ++i){
		sum += time[i];
	}

	std::vector<double> rate(numProcs, 0);

	for ( size_t i = 1; i < time.size(); ++i){
		rate[i] = (double) sum / time[i];
		//rate[i] = (double) sum / (time[i]*log(time[i]));
	}

	double powersum = 0;
	for( size_t i = 1; i < numProcs; ++i){
		powersum += rate[i] ;
	}

	for ( size_t i = 1; i < time.size(); ++i){
		currentWeights[i] = rate[i] / powersum;
	}
}

void faster::fastScheduler::printProcstats(fastTask * task){
	double mram=0, mutime=0, mstime=0;
	for ( size_t i = 1; i < task->procstats.size(); i++ ){
		mram += task->procstats[i].ram;
		mutime += task->procstats[i].utime;
		mstime += task->procstats[i].stime;
	}
	mram /= task->procstats.size()-1;
	mutime /= task->procstats.size()-1;
	mstime /= task->procstats.size()-1;

	//fprintf(stderr, "\033[1;34m%4.1lf %3.1lf %3.1lf ", mram, mutime, mstime);
	std::cerr << std::fixed << std::setprecision(1)
		<< std::setw(6) << mram << " "
		<< std::setw(5) << mutime << " " << mstime << " ";

	std::cerr << "\033[0m| " ;
	for ( size_t i = 0; i < task->procstats.size(); i++ ){
		//fprintf(stderr, "%4.1lf ", task->procstats[i].ram);
		std::cerr << std::setw(6) << task->procstats[i].ram << " ";
	}

}

void faster::fastScheduler::printTaskInfo(size_t taskID){
	auto task = taskList[taskID];
	std::vector<size_t> t = task->times;
	t.erase(t.begin());
	double m = mean(t);
	double sd = stdDev(t, m);


	std::cerr << "\033[1;34m " ;
	//fprintf(stderr, "%2ld ", task->id);
	std::cerr << std::setw(2) << task->id << " ";
	if ((task->operationType & (OP_GENERICREDUCE | OP_GENERICMAP | OP_GENERICUPDATE) ) && ((*funcName)[task->functionId].size() > 0)){
		std::cerr << decodeOptypeAb(task->operationType) << " " ;
		std::cerr << std::setw(9) << (*funcName)[task->functionId] << "\t";
	}else{
		std::cerr << std::setw(15) << decodeOptype(task->operationType) << "\t";
	}

	std::cerr << "\033[0m " ;
	//fprintf(stderr, "%2d %2ld %2ld ", task->functionId, task->srcFDD, task->destFDD );
	std::cerr << std::setw(2) << task->functionId << " "
		<< task->srcFDD << " "
		<< task->destFDD << " ";

	std::cerr << "| \033[1;31m " ;
	//fprintf(stderr, "%5ld %6.1lf %3.1lf ", task->duration, m, sd/m);
	std::cerr << std::setprecision(1)
		<< std::setw(5) << task->duration << " "
		<< std::setw(8) << m << " "
		<< std::setw(5) << sd/m << " ";

	std::cerr << "\033[0m| " ;
	printProcstats(task);
	std::cerr << "| " ;

	for ( auto it2 = t.begin() ; it2 != t.end(); it2++){
		//fprintf(stderr, "%5ld ", *it2);
		std::cerr << std::setw(2) << *it2 << " ";
	}

	std::cerr << "\n";
}
void faster::fastScheduler::printHeader(){
	std::cerr << "\033[1;34mID# Task\033[0m Func Src Dest | \033[1;31mDuration(ms) Avg_Proc_Time PT_CV \033[0m | RAM | Individual_Processing_Times\n";
}

void faster::fastScheduler::printTaskInfo(){

	double cvsum = 0;
	size_t mm = 0;
	int count = 0;

	printHeader();

	//for ( auto it = taskList.begin(); it != taskList.end(); it++){
	for ( size_t i = 0; i < taskList.size(); ++i ){
		std::vector<size_t> t = (taskList[i])->times;
		t.erase(t.begin());
		double m = mean(t);
		double sd = stdDev(t, m);

		mm += m;
		if( m > 100 ){
			cvsum += sd/m;
			count++;
		}

		printTaskInfo(i);
	}
	std::cerr << "\n av_Time:" << mm/taskList.size() << " av_CV:" << cvsum/count << "\n";
}


void faster::fastScheduler::updateTaskInfo(){
	while (  infoPos < taskList.size() ){
		printTaskInfo(infoPos++);
	}
}

