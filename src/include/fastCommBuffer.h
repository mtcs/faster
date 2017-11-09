#ifndef LIBFASTER_FASTCOMMBUFFER_H
#define LIBFASTER_FASTCOMMBUFFER_H


#include <iostream>
#include <algorithm>
#include <cstring>
#include <vector>
#include <tuple>
#include <unordered_map>

#include "misc.h"

namespace faster {
	const int BUFFER_INITIAL_SIZE = 512*1024;

	class fastCommBuffer{
		private:
			char * _data;
			size_t _size;
			size_t _allocatedSize;
			bool _ownData;

		public:
			fastCommBuffer();
			fastCommBuffer(size_t s);
			~fastCommBuffer();

			void setBuffer(void * buffer, size_t s);
			void reset();

			char * data();
			char * pos();
			char * pos(size_t pos);
			size_t size();
			size_t free();
			void advance(size_t pos);

			void grow(size_t s);

			void print();

			// WRITE Data
			template <typename T>
			void write(const T & v);
			template <typename T>
			void write(const T &v, size_t s);
			template <typename T>
			void writePos(const T &v, size_t s, size_t pos);
			template <typename T>
			void writePos(const T &v, size_t pos);
			template <typename T>
			inline void writeSafe(const T * v, size_t s);
			template <typename T>
			inline void write(const T * v, size_t s);

			void write(const std::string & str);
			void write(const std::vector<std::string> & v);
			template <typename T>
			void write(const std::vector<T> & v);
			template <typename K, typename T>
			void write(const std::pair<K,T> & p);
			template <typename K, typename T, typename U>
			void write(const std::tuple<K,T,U> & t);
			template <typename K, typename U>
			void write(const std::unordered_map<K,U> & m);// */
			template <typename K, typename U>
			void write(const std::unordered_map<K,U> & m, size_t s);// */

			void read(procstat &s);
			void write(const procstat &s);
			void writePos(const procstat &s, size_t pos);
			void advance(procstat &s);

			// -------------- READ Data -------------- //
			template <typename T>
			void read(T & v, size_t s);
			template <typename T>
			void read(T * v, size_t s);
			template <typename T>
			void read(T & v);
			void readString(std::string & v, size_t s);
			void read(std::string & s);
			void read(std::vector<std::string> & s);
			template <typename T>
			void readVec(std::vector<T> & v, size_t s);
			template <typename T>
			void read(std::vector<T> & v);
			template <typename K, typename T>
			void read(std::pair<K,T> & p);
			template <typename K, typename T, typename U>
			void read(std::tuple<K,T,U> & t);
			template <typename K, typename U>
			void read(std::unordered_map<K,U> & m);

			// Operators

			template <typename T>
			fastCommBuffer & operator<<(const T & v){
				write(v);
				return *this;
			}
			template <typename K, typename T>
			fastCommBuffer & operator<<(const std::unordered_map<K,T> & v){
				//std::cerr << "\033[1;32m<<UMAP\033[0m";
				write<K,T>(v);
				return *this;
			}

			template <typename T>
			fastCommBuffer & operator>>(T & v){
				read(v);
				return *this;
			}

			template <typename K, typename T>
			fastCommBuffer & operator>>(std::unordered_map<K,T> & v){
				read<K,T>(v);
				return *this;
			}


	};

	template <typename T>
	void fastCommBuffer::write(const T & v){
		//std::cerr << "write var generic: " << sizeof(T) << "\n";
		//std::cerr << "\033[1;31mGW\033[0m";
		write<T>( v, sizeof(T) );
	}

	template <typename T>
	void fastCommBuffer::write(const T &v, size_t s){
		grow(_size + s);
		std::copy_n( (char*)&v, s , _data + _size );

		_size += s;
	}
	template <typename T>
	void fastCommBuffer::writePos(const T &v, size_t s, size_t pos){
		grow(pos + s);
		std::copy_n((char*) &v, s, _data + pos);

		if(_size < pos+s)
			_size = pos+s;
	}
	template <typename T>
	void fastCommBuffer::writePos(const T &v, size_t pos){
		writePos(v, sizeof(T), pos);
	}

	template <typename T>
	inline void fastCommBuffer::writeSafe(const T * v, size_t s){
		//std::cerr << "Write safe buffer: " << s << "\n";
		std::copy_n( (char*)v, s, &_data[_size]);

		_size += s;
	}
	template <typename T>
	inline void fastCommBuffer::write(const T * v, size_t s){
		//std::cerr << "write buffer: " << s << "\n";
		grow(_size + s);
		writeSafe(v, s);
	}


	template <typename T>
	void fastCommBuffer::write(const std::vector<T> & v){
		size_t s = v.size();
		grow(_size + sizeof(size_t) + s * sizeof(T));
		writeSafe( &s, sizeof(size_t) );
		writeSafe( v.data(), s * sizeof(T) );
	}

	template <typename K, typename T>
	void fastCommBuffer::write(const std::pair<K,T> & p){
		//std::cerr << "\033[1;34mPW\033[0m";
		write(p.first);
		write(p.second);
	}

	template <typename K, typename T, typename U>
	void fastCommBuffer::write(const std::tuple<K,T,U> & t){
		write(std::get<0>(t));
		write(std::get<1>(t));
		write(std::get<2>(t));
	}

	template <typename K, typename U>
	void fastCommBuffer::write(const std::unordered_map<K,U> & m, size_t s){
		s = m.size();
		//std::cerr << "write umap2 size: " << m.size() << "\n";
		grow(sizeof(size_t)+m.size()*(sizeof(K)+sizeof(U)));
		write(s);
		for ( const std::pair<K,U> & it : m ){
			write<K,U>(it);
		}
	}

	template <typename K, typename U>
	void fastCommBuffer::write(const std::unordered_map<K,U> & m){
		size_t s = m.size();
		//std::cerr << "write umap size: " << m.size() << "\n";
		grow(sizeof(size_t)+m.size()*(sizeof(K)+sizeof(U)));
		write(s);
		if ( s == 0 ) return;
		for ( const std::pair<K,U> & it : m ){
			write<K,U>(it);
		}
	}


	template <typename T>
	void fastCommBuffer::read(T & v, size_t s){
		std::copy_n(_data + _size, s, (char*) &v);
		_size += s;
	}
	template <typename T>
	void fastCommBuffer::read(T * v, size_t s){
		//std::cerr << "read buffer: " << s << "\n";
		std::copy_n(_data + _size, s, (char*) v);
		_size += s;
	}
	template <typename T>
	void fastCommBuffer::read(T & v){
		read( &v, sizeof(T) );
	}
	template <typename T>
	void fastCommBuffer::readVec(std::vector<T> & v, size_t s){
		v.resize(s);
		size_t stride = s*sizeof(T);
		//v.assign((T*) &_data[_size], (T*) &_data[_size + stride] );
		std::copy_n(_data + _size, stride, (char*) v.data());
		//std::copy_n((T*) _data + _size, s,  v.data());
		_size += stride;
	}
	template <typename T>
	void fastCommBuffer::read(std::vector<T> & v){
		size_t size;
		read(size);
		readVec(v, size);
	}
	template <typename K, typename T>
	void fastCommBuffer::read(std::pair<K,T> & p){
		//std::cerr << "\033[1;34mPR\033[0m";
		read(p.first);
		read(p.second);
	}
	template <typename K, typename T, typename U>
	void fastCommBuffer::read(std::tuple<K,T,U> & t){
		K k;
		T type;
		U s;
		read(k);
		read(type);
		read(s);
		t = std::make_tuple (k,type,s);
	}
	template <typename K, typename U>
	void fastCommBuffer::read(std::unordered_map<K,U> & m){
		size_t s;
		read(s);
		//std::cerr << "read umap size: " << s << "\n";
		if (s == 0) return;
		m.reserve(1.1*(m.size()+s));
		for (size_t i = 0; i < s; i++){
			//std::cerr << i;
			std::pair<K,U> p;
			read(p);
			m.insert(std::move(p));
		}
	}

}

#endif
