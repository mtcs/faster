#include "fastCommBuffer.h"

faster::fastCommBuffer::fastCommBuffer(){
	_size = 0;
	_allocatedSize = BUFFER_INITIAL_SIZE;
	//_data = new char [BUFFER_INITIAL_SIZE];
	_data = (char*) std::malloc(BUFFER_INITIAL_SIZE);
	_ownData = true;
}
faster::fastCommBuffer::fastCommBuffer(size_t s){
	_size = 0;
	if (s > 0){
		_allocatedSize = s;
		//_data = new char [_allocatedSize];
		_data = (char*) std::malloc(_allocatedSize);
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
		//delete [] _data;
		std::free(_data);
	}
}

void faster::fastCommBuffer::setBuffer(void * buffer, size_t s){
	//if (_data)
	//	delete [] _data;
	if ((_data != NULL) && (_ownData))
		std::free(_data);
	_ownData = false;
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
		_allocatedSize = std::max(size_t(1.5*_allocatedSize), 2 + s + _allocatedSize);

		_data = (char*) std::realloc(_data, _allocatedSize);
		//char * newdata = new char[_allocatedSize];

		//memcpy(newdata, _data, _size );
		//std::copy(_data, _data+_size, newdata);
		//*newdata = std::move(*_data);

		//delete [] _data;

		//_data = newdata;
		//std::cerr << _allocatedSize<< ")";
	}
}

void faster::fastCommBuffer::print(){
	for (size_t i = 0; i < _size; ++i){
		std::cout << (int) _data[i] << ' ';
	}
}

void faster::fastCommBuffer::write(const std::string & str){
	size_t s = str.length();
	//std::cerr << "\033[1;35mSW\033[0m";
	//std::cerr << str.size() << " ";
	//size_t alignedSize = s;
	//short missaligment = (s & 0x03);
	//if ( missaligment ){
	//	alignedSize += 4 - missaligment;
	//}
	//grow(_size + sizeof(size_t) + alignedSize );
	grow(_size + sizeof(size_t) + s );
	writeSafe( &s, sizeof(size_t) );
	writeSafe( str.data(), s );
	//if ( missaligment ){
	//	advance(4 - missaligment);
	//}
}

void faster::fastCommBuffer::write(const std::vector<std::string> & v){
	size_t s = v.size();
	grow(_size + sizeof(size_t) + s );
	writeSafe( &s, sizeof(size_t) );
	for ( auto i : v ){
		write( i );
	}
}
/*template <>
void faster::fastCommBuffer::write(void * v, size_t s){
	std::memcpy( &_data[_size], v, s );
	_size += s;
}// */

void faster::fastCommBuffer::readString(std::string & v, size_t s){
	//std::cerr << "\033[1;35mSR\033[0m";
	//short missaligment = (s & 0x03);
	//size_t stride;
	v.resize(s);
	//stride = s;
	std::copy_n( _data + _size, s, (char*) v.data() );
	//std::cerr << v.size() << " ";
	//if ( missaligment )
	//	stride += (4 - missaligment);
	//_size += stride;
	_size += s;
}

void faster::fastCommBuffer::read(std::string & s){
	size_t size;
	read(size);
	readString(s, size);
}

void faster::fastCommBuffer::read(std::vector<std::string> & v){
	size_t s;
	read(s);
	v.resize(s);
	for (size_t i = 0 ; i < s ; i++){
		read(v[i]);
	}
}

void faster::fastCommBuffer::read(procstat &s){
	read(s.ram);
	read(s.utime);
	read(s.stime);
}

void faster::fastCommBuffer::write(const procstat &s){
	write(s.ram);
	write(s.utime);
	write(s.stime);
}

void faster::fastCommBuffer::writePos(const procstat &s, size_t pos){
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
