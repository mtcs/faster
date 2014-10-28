#include <vector>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <utility>
#include <stdio.h>

#include "fastScheduler.h"
#include "fastTask.h"
#include "misc.h"

faster::fastScheduler::fastScheduler(unsigned int numProcs){
	this->numProcs = numProcs;
	numTasks = 0;
	resetProcessWeights();
	_dataMigrationNeeded = false;
	infoPos = 0;
}
faster::fastScheduler::~fastScheduler(){
	for ( auto it = taskList.begin(); it != taskList.end() ; it++ ){
		for (size_t i = 0; i < (*it)->globals.size(); i++){
			delete [] (char*) (*it)->globals[i].first;
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
			delta[i].second = lastTask->size*(thisTask->allocation[i] - lastTask->allocation[i]);
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

void faster::fastScheduler::updateWeights(){
	unsigned int i;
	if (taskList.size() > 0){
		fastTask * lastTask = taskList.back();
		std::vector<double> rate(numProcs, 0);

		std::cerr << "      [ Exec.Times: ";
		for( i = 1; i < numProcs; ++i){
			std::cerr << lastTask->times[i] << " ";
			rate[i] = lastTask->allocation[i]/(double)lastTask->times[i];
		}
		std::cerr << " ]\n";

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


double * faster::fastScheduler::getNewAllocation(){
	double * r;
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

	if ( (taskList.size() > 0) && ( ! _dataMigrationNeeded) )
		//r = taskList.back()->allocation;
		r = taskList.back()->allocation;
	else
		r = new double[numProcs];
	
	if ( (taskList.size() > 0) && ( _dataMigrationNeeded) )
		updateWeights();

	r[0] = 0;
	//std::cerr << "      [ Processes Weights: ";
	for( size_t i = 1; i < numProcs; ++i){
		r[i] = currentWeights[i];
		//std::cerr << r[i] << " ";
	}
	//std::cerr << "]\n";


	return r;
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size, std::vector< std::pair<void*, size_t> > & globalTable){
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
	for ( size_t i = 0; i < globalTable.size(); i++){
		void * var = new char[globalTable[i].second];

		std::memcpy(var, globalTable[i].first, globalTable[i].second);

		newTask->globals.insert(newTask->globals.end(), std::make_pair(var, globalTable[i].second) );
	}

	taskList.insert(taskList.end(), newTask);

	return newTask;
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int sid, size_t size, std::vector< std::pair<void*, size_t> > & globalTable){
	return enqueueTask(opT, sid, 0, -1, size, globalTable );
}

void faster::fastScheduler::taskProgress(unsigned long int tid, unsigned long int pid, size_t time){
	taskList[tid]->workersFinished++;

	taskList[tid]->times[pid] = time;
}

void faster::fastScheduler::taskFinished(unsigned long int tid, size_t time){
	taskList[tid]->duration = time;
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

void faster::fastScheduler::printTaskInfo(size_t taskID){
	auto task = taskList[taskID];
	std::vector<size_t> t = task->times;
	t.erase(t.begin());
	double m = mean(t);
	double sd = stdDev(t, m);


	std::cerr << "\033[1;34m" ;
	fprintf(stderr, "%2ld ", task->id);
	std::cerr << decodeOptype(task->operationType) << "\t";

	std::cerr << "\033[0m" ;
	fprintf(stderr, "%2d %2ld %2ld ", task->functionId, task->srcFDD, task->destFDD );

	std::cerr << "| \033[1;31m" ;
	fprintf(stderr, "%5ld %6.1lf %3.1lf ", task->duration, m, sd/m);

	std::cerr << "\033[0m| " ;

	for ( auto it2 = t.begin() ; it2 != t.end(); it2++){
		fprintf(stderr, "%5ld ", *it2);
	}

	std::cerr << "\n";
}
void faster::fastScheduler::printHeader(){
	std::cerr << "\033[1;34mID# Task\033[0m Func Src Dest | \033[1;31mDuration(ms) Avg_Proc_Time PT_CV \033[0m| Individual_Processing_Times\n";
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

