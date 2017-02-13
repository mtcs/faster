#include <iostream>
#include <algorithm>
#include <cmath>

#include <string>
#include <unordered_map>

#include "hdfs.h"
#include "hdfsEngine.h"

faster::hdfsEngine::hdfsEngine(){
	_ready = false;
	_fs = hdfsConnect("default", 0);
	_ready = true;
}
faster::hdfsEngine::~hdfsEngine(){
	hdfsDisconnect((hdfsFS) _fs);
}

bool faster::hdfsEngine::isReady(){
	return _ready;
}

bool faster::hdfsEngine::isConnected(){
	return (_fs != NULL);
}

faster::hdfsFile faster::hdfsEngine::open(std::string path, fileMode mode){
	return hdfsFile((hdfsFS) _fs, path, mode);
}

void faster::hdfsEngine::close(faster::hdfsFile & f){
	f.close();
}

void faster::hdfsEngine::del(std::string path){
	hdfsDelete((::hdfsFS)_fs, path.data(), 1);
}

bool faster::hdfsEngine::exists(std::string path){
	int r = hdfsExists((::hdfsFS) _fs, path.data());
	return (r == 0);
}

faster::hdfsFile::hdfsFile(void * fs, std::string & path, fileMode mode){
	_fs = fs;
	_path = path;
	_f = hdfsOpenFile((::hdfsFS) fs, path.data(), mode,0,0,0);
	_open = true;
	_buffer.resize(128*1024);
	_path = path;

	_readBuffer = new char[_readBufferSize];
	_readSize = 0;
	_readPos = _readBufferSize;
	_readOffset = 0;
	_eof = false;

}
faster::hdfsFile::~hdfsFile(){
	delete [] _readBuffer;
	close();
}
void faster::hdfsFile::close(){
	if (_open){
		hdfsCloseFile((::hdfsFS) _fs, (::hdfsFile) _f);
		_open = false;
	}
}
bool faster::hdfsFile::good(){
	return ( (_f) && (! _eof) );
}

size_t faster::hdfsFile::read(char * v, size_t n){
	return hdfsRead((::hdfsFS)_fs, (::hdfsFile)_f, (void*) v, n);
}

size_t faster::hdfsFile::write(char * v, size_t n){
	return hdfsWrite((::hdfsFS)_fs, (::hdfsFile)_f, (void*) v, n);
}

size_t faster::hdfsFile::seek(size_t offset){
	return hdfsSeek((::hdfsFS)_fs, (::hdfsFile)_f, (tOffset) offset);
}

std::string faster::hdfsFile::getLine(char sep){
	int count = 0;
	//std::cerr << "READ Line "; std::cerr.flush();
	std::string retLine = "";
	size_t pos = _readPos;
	bool foundSep = false;

	do{
		// Read a chunk if needed
		if ((long int)_readPos >= _readSize){
			//std::cerr << "READ Chunk "; std::cerr.flush();
			_readSize = read(_readBuffer, _readBufferSize);
			//std::cerr << " (" << _readSize << ") "; std::cerr.flush();
			// End Of File
			if (_readSize <= 0){
				//std::cerr << "DONE REDING FILE";
				_eof = true;
				break;
			}
			_readPos = 0;
			pos = 0;
			_readOffset += _readSize;
		}
		// Find the separator in this chunk
		while ((pos < _readBufferSize) && ((long int)pos < _readSize)){
			if(_readBuffer[pos] == sep){
				foundSep = true;
				break;
			}

			pos++;
		}
		// Copy data to output
		size_t length = pos - _readPos;
		//std::cerr << "[" << _readPos << "," << pos << "] length: "<< length << " "; std::cerr.flush();

		if (length > 0){
			retLine.append(&_readBuffer[_readPos], length);
			//std::cerr << "\""<< retLine << "\" ";
			count += length;
		}
		_readPos = pos + 1;
	}while (! foundSep);
	//std::cerr << "\n"; std::cerr.flush();

	return retLine;
}

std::string faster::hdfsFile::getLine(size_t offset, char sep){
	seek(offset);
	return getLine(sep);
}

std:: vector<std::deque<int>>  faster::hdfsFile::getBlocksLocations(){
	std::vector<std::deque<int>> loc;
	std::unordered_map<std::string,int> hostMap;
	//int i = 0;
	int hostsMapped = 0;

	hdfsFileInfo * info = hdfsGetPathInfo((::hdfsFS)_fs, _path.data());

	//std::cerr << "Getting Blocks Locations for: " << _path << " (" << info->mSize << " bytes)";
	char *** blocksLoc = hdfsGetHosts((::hdfsFS)_fs, _path.data(), 0, info->mSize);
	int numBlocks = std::ceil((float)info->mSize/info->mBlockSize);
	loc.resize(numBlocks);

	//std::cerr << ".\n";
	for ( int i = 0; i < numBlocks ; i++){
	//while ( blocksLoc[i] != NULL){
		int j = 0;
		char ** hostList = blocksLoc[i];
		//std::cerr << "   (" << i << ") ";
		while ( hostList[j] != NULL ){
			//std::cerr << hostList[j] << " ";
			auto search = hostMap.find(hostList[j]);
			if ( search == hostMap.end()){
				hostMap[hostList[j]] = hostsMapped ++;
			}
			auto & locList = loc[i];
			locList.insert(locList.end(), hostMap[hostList[j]]);
			j++;
		}
		//i++;
		//std::cerr << "\n";
	}

	hdfsFreeFileInfo(info,1);
	hdfsFreeHosts(blocksLoc);

	return loc;
}

void faster::hdfsFile::del(){
	close();
	hdfsDelete((::hdfsFS)_fs, _path.data(), 1);
}
