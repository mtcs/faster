#include <string>

namespace faster{
	template <typename K>
	class hasher{
		private:
			int _spectrum;
		public:
			hasher(int spectrum){
				_spectrum = spectrum;
			}
			int get ( K key ){
				return key % _spectrum;
				//return ( key >> 6 ) % _spectrum;
			}

	};
	template <>
	class hasher<float>{
		private:
			int _spectrum;
		public:
			hasher(int spectrum){
				_spectrum = spectrum;
			}
			int get ( float key ){
				return ((int) key) % _spectrum;
			}

	};
	template <>
	class hasher<double>{
		private:
			int _spectrum;
		public:
			hasher(int spectrum){
				_spectrum = spectrum;
			}
			int get ( double key ){
				return ((long long) key) % _spectrum;
			}

	};
	template <>
	class hasher<std::string>{
		private:
			int _spectrum;
		public:
			hasher(int spectrum){
				_spectrum = spectrum;
			}
			int get ( std::string key ){
				unsigned int hash = 5381;
				for ( size_t i = 0; i < key.size(); i++){
					hash = ((hash << 5) + hash) + key[i];
				}
				return ( hash ) % _spectrum;
			}

	};
}
