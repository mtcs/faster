#include <iostream>
#include <string>

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

faster::hdfsFile::hdfsFile(void * fs, std::string path, fileMode mode){
	_fs = fs;
	_f = hdfsOpenFile((::hdfsFS) fs, path.data(), mode,0,0,0);
	_open = true;
	_buffer.resize(128*1024);
	_path = path;
}
faster::hdfsFile::~hdfsFile(){
	close();
}
void faster::hdfsFile::close(){
	if (_open){
		hdfsCloseFile((::hdfsFS) _fs, (::hdfsFile) _f);
		_open = false;
	}
}

size_t faster::hdfsFile::read(char * v, size_t n){
	return hdfsRead((::hdfsFS)_fs, (::hdfsFile)_f, (void*) v, n);
}

size_t faster::hdfsFile::write(char * v, size_t n){
	return hdfsWrite((::hdfsFS)_fs, (::hdfsFile)_f, (void*) v, n);
}

void faster::hdfsFile::del(){
	close();
	hdfsDelete((::hdfsFS)_fs, _path.data(), 1);
}
