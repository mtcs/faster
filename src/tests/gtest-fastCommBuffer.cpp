#include <iomanip>

#include "gtest/gtest.h"
#include "fastCommBuffer.h"

namespace testfaster{

TEST(testFastComBufferCreate, CreateEmpty){
	faster::fastCommBuffer buff(0);

	EXPECT_EQ(0, buff.size())
		<< "Empty buffer has size != 0";
	EXPECT_STREQ(NULL, buff.data())
		<< "Empty buffer has internal buffer not empty";
	ASSERT_EQ(buff.data(), buff.pos())
		<< "Empty buffer has pos != buffer start";
}


template <int NUMITEMS = 10*1000>
class testFastComBuffer : public ::testing::Test {
	public:
		template <typename T>
		void comp(T & a, T & b){
			ASSERT_EQ(a,b)
				//<< "Buffer read wrong input ( " << std::hex<< a << " != " << std::hex<< b << ")";
				<< "Buffer read wrong input";
		}
		template <typename T>
		void comp(std::pair<T,T> & a, std::pair<T,T> & b){
			ASSERT_EQ(a,b)
				<< "Buffer read wrong input";
		}
		template <typename T>
		void comp(std::tuple<T,T,T,T> & a, std::tuple<T,T,T,T> & b){
			ASSERT_EQ(a,b)
				<< "Buffer read wrong input";
		}
		template <typename T>
		void comp(std::vector<T> & a, std::vector<T> & b){
			for ( size_t i = 0; i < a.size(); i++){
				ASSERT_EQ(a[i], b[i])
					<< "Buffer read wrong input ( " << a[i] << " != " << b[i] << ")";
			}
		}
		void comp(std::vector<std::string> & a, std::vector<std::string> & b){
			for ( size_t i = 0; i < a.size(); i++){
				ASSERT_STREQ(a[i].data(), b[i].data())
					<< "Buffer read wrong input ( " << a[i] << " != " << b[i] << ")";
			}
		}
		template <typename T>
		void testWrite(T & val, const char * result, int size){
			buff << val;

			EXPECT_EQ(size, buff.size())
				<< "Buffer has size != " << size;

			std::string d(buff.data(),size);
			for ( int i = 0; i < size; i++){
				ASSERT_EQ(d.data()[i], result[i])
					<< "[" << i << "] Buffer (" << d << ") ";
			}

			EXPECT_EQ(buff.data() + size, buff.pos())
				<< "Buffer has pos != " << size;


			T ret;
			buff.reset();
			ASSERT_EQ(0, buff.size())
				<< "Buffer size after reset != 0";
			buff >> ret;
			comp(ret, val);

		}


	protected:

		faster::fastCommBuffer buff;

		virtual void SetUp() { }
		virtual void TearDown() { }


};
typedef testFastComBuffer<10*1000> testFastComBuffer10k;

TEST_F(testFastComBuffer10k, CreateFixSize){
	EXPECT_EQ(0, buff.size())
		<< "Buffer has size != 0";
	EXPECT_EQ(512*1024, buff.free())
		<< "Buffer has size != 0";
	ASSERT_EQ(buff.data(), buff.pos())
		<< "Buffer has pos != buffer start";
}

TEST_F(testFastComBuffer10k, advance){
	buff.advance(1024);

	EXPECT_EQ(1024, buff.size())
		<< "Buffer has size != 0";
	ASSERT_EQ(buff.data()+1024, buff.pos())
		<< "Buffer has pos != buffer start";
}

TEST_F(testFastComBuffer10k, grow){
	buff.grow(1024*1024);

	EXPECT_EQ(0, buff.size())
		<< "Buffer has size != 0";
	EXPECT_EQ((512+1024)*1024, buff.free())
		<< "Buffer has size != 0";
	ASSERT_EQ(buff.data(), buff.pos())
		<< "Buffer has pos != buffer start";
}

TEST_F(testFastComBuffer10k, writeShort){
	short val = 0x23be;
	testWrite(val, "\xBE#", 2);
}

TEST_F(testFastComBuffer10k, writeInt){
	int val = 0xFAFCDCEF;
	testWrite(val, "\xEF\xDC\xFC\xFA", 4);
}

TEST_F(testFastComBuffer10k, writeLong){
	long val = 123456789123456789;
	testWrite(val, "\x15_\xD0\xACK\x9B\xB6\x1", 8);
}

TEST_F(testFastComBuffer10k, writeFloat){
	float val = 12.34;
	testWrite(val, "\xA4pEA", 4);
}

TEST_F(testFastComBuffer10k, writeDouble){
	double val = 1234.56789;
	testWrite(val, "\xE7\xC6\xF4\x84" "EJ\x93@", 8);
}

TEST_F(testFastComBuffer10k, writeString){
	std::string val = "paralelepipedo aloprado confuso";
	testWrite(val, "\x1F\0\0\0\0\0\0\0paralelepipedo aloprado confuso", val.length()+8);
}

// Containers
TEST_F(testFastComBuffer10k, writeVecShort){
	std::vector<short> val = {1,2,3,4,5,6};
	testWrite(val, "\x6\0\0\0\0\0\0\0\x1\0\x2\0\x3\0\x4\0\x5\0\x6\0", 20);
}

TEST_F(testFastComBuffer10k, writeVecInt){
	std::vector<int> val = {1,2,3,4,5,6};
	testWrite(val, "\x6\0\0\0\0\0\0\0\x1\0\0\0\x2\0\0\0\x3\0\0\0\x4\0\0\0\x5\0\0\0\x6\0\0\0", 8+6*4);
}

TEST_F(testFastComBuffer10k, writeVecLong){
	std::vector<long> val = {1,2,3};
	testWrite(val, "\x3\0\0\0\0\0\0\0\x1\0\0\0\0\0\0\0\x2\0\0\0\0\0\0\0\x3\0\0\0\0\0\0\0", 8+3*8);
}

TEST_F(testFastComBuffer10k, writeVecString){
	std::vector<std::string> val = {"GG", "GG", "GG"};
	testWrite(val, "\x03\0\0\0\0\0\0\0\x02\0\0\0\0\0\0\0GG\x02\0\0\0\0\0\0\0GG\x02\0\0\0\0\0\0\0GG", 8+ 3*(2+8));
}

TEST_F(testFastComBuffer10k, writePairShort){
	std::pair<short,short> val = {0x2323,0x2323};
	testWrite(val, "####", 4);
}

TEST_F(testFastComBuffer10k, writePairInt){
	std::pair<int,int> val = {0xFAFCDCEF,0xFAFCDCEF};
	testWrite(val, "\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA", 8);
}

TEST_F(testFastComBuffer10k, writeTupleInt4){
	std::tuple<int,int,int,int> val = std::make_tuple(0xFAFCDCEF,0xFAFCDCEF,0xFAFCDCEF,0xFAFCDCEF);
	testWrite(val, "\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA", 16);
}

/*TEST_F(testFastComBuffer10k, writeCustomTuple){
	std::tuple<int,double,float,bool> val = {
			0xFAFCDCEF,
			0xFAFCDCEFFAFCDCEF,
			0xFAFCDCEF,
			false
		};
	testWrite(val, "\0\x18\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA", 24);
}// */

/*typedef struct{
	int a;
	float b;
}customStructData_t;

TEST_F(testFastComBuffer10k, writeCustomStruct){
	customStructData_t val;
	val.a = 0xFAFCDCEF;
	val.b = 0xFAFCDCEF;
	testWrite(val, "\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA", 16);
}// */

/*TEST_F(testFastComBuffer10k, writeTupleFloat4){
	std::tuple<float,float,float,float> val = {0xFAFCDCEF,0xFAFCDCEF,0xFAFCDCEF,0xFAFCDCEF};
	testWrite(val, "\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA\xEF\xDC\xFC\xFA", 16);
}// */

	int main(int argc, char **argv) {
		  ::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();
	}
};
