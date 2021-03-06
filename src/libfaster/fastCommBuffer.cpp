#include "fastCommBuffer.h"

faster::fastCommBuffer::fastCommBuffer(){
	_size = 0; 
	_allocatedSize = BUFFER_INITIAL_SIZE;
	_data = new char [BUFFER_INITIAL_SIZE];
	_ownData = true;
}
faster::fastCommBuffer::fastCommBuffer(size_t s){
	_size = 0; 
	if (s > 0){
		_allocatedSize = s;
		_data = new char [_allocatedSize];
		_ownData = true;
	}else{
		_allocatedSize = 0;
		_data = NULL;
		_ownData = false;
	}
}
faster::fastCommBuffer::~fastCommBuffer(){
	if ((_data != NULL) && (_ownData)){
		//std::cerr << "DEL _DATA\n";
		delete [] _data;
	}
}

void faster::fastCommBuffer::setBuffer(void * buffer, size_t s){ 
	//if (_data)
	//	delete [] _data;
	_size = 0; 
	_data = (char*) buffer;
	_allocatedSize = s;
}
void faster::fastCommBuffer::reset(){ 
	_size = 0; 
}

char * faster::fastCommBuffer::data(){ 
	return _data; 
}
char * faster::fastCommBuffer::pos(size_t pos){ 
	return &(_data[pos]); 
}
char * faster::fastCommBuffer::pos(){ 
	return &(_data[_size]); 
}
size_t faster::fastCommBuffer::size(){ 
	return _size; 
}
size_t faster::fastCommBuffer::free(){ 
	return _allocatedSize - _size; 
}
void faster::fastCommBuffer::advance(size_t pos){
	_size += pos; 
}

void faster::fastCommBuffer::grow(size_t s){
	if (_allocatedSize < s){
		//std::cerr << "(GROW BUFFER: "<< _allocatedSize<< " > ";
		_allocatedSize = std::max(size_t(1.5*_allocatedSize), s + _allocatedSize);
		
		char * newdata = new char[_allocatedSize];
		
		//memcpy(newdata, _data, _size );
		std::copy(_data, _data+_size, newdata);

		delete [] _data;

		_data = newdata;
		//std::cerr << _allocatedSize<< ")";
	}
}

void faster::fastCommBuffer::print(){
	for (size_t i = 0; i < _size; ++i){
		std::cout << (int) _data[i] << ' ';
	}
}

void faster::fastCommBuffer::read(procstat &s){
	read(s.ram);
	read(s.utime);
	read(s.stime);
}

void faster::fastCommBuffer::write(procstat &s){
	write(s.ram);
	write(s.utime);
	write(s.stime);
}

void faster::fastCommBuffer::writePos(procstat &s, size_t pos){
	size_t save = _size;
	_size = pos;
	write(s);
	_size = save;
}

void faster::fastCommBuffer::advance(procstat &s){
	advance (sizeof(s.ram));
	advance (sizeof(s.utime));
	advance (sizeof(s.stime));
}
