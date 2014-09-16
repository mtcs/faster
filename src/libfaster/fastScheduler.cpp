#include <vector>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <utility>

#include "fastScheduler.h"
#include "fastTask.h"
#include "misc.h"

faster::fastScheduler::fastScheduler(unsigned int numProcs){
	this->numProcs = numProcs;
	numTasks = 0;
	resetProcessWeights();
	_dataMigrationNeeded = false;
}
faster::fastScheduler::~fastScheduler(){
	for ( auto it = taskList.begin(); it != taskList.end() ; it++ )
		delete (*it);
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

std::vector<std::list< std::pair<int,long int> >> faster::fastScheduler::getDataMigrationInfo(){
	std::vector<std::list< std::pair<int,long int> >> r (numProcs, std::list<std::pair<int,long int>>());
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
		std::cerr << "      [ Time mean:" << m << " VC:" << sd/m << " ]\n";
		if ( (m > 1) && ( sd/m > 1 ) ){
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
	std::cerr << "      [ Processes Weights: ";
	for( size_t i = 1; i < numProcs; ++i){
		r[i] = currentWeights[i];
		std::cerr << r[i] << " ";
	}
	std::cerr << "]\n";


	return r;
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size){
	fastTask * newTask = new fastTask();
	newTask->id = numTasks++;
	newTask->srcFDD = idSrc;
	newTask->destFDD = idRes;
	newTask->size = size;
	newTask->operationType = opT;
	newTask->functionId = funcId;
	newTask->workersFinished = 0;
	newTask->allocation = getNewAllocation();
	newTask->times = std::vector<size_t>(numProcs, 0);

	taskList.insert(taskList.end(), newTask);

	return newTask;
}

faster::fastTask * faster::fastScheduler::enqueueTask(fddOpType opT, unsigned long int id, size_t size){
	return enqueueTask(opT, id, 0, -1, size);
}

void faster::fastScheduler::taskProgress(unsigned long int id, unsigned long int pid, size_t time){
	taskList[id]->workersFinished++;
	taskList[id]->times[pid] = time;
}


