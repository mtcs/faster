#include <string>
#include "gtest/gtest.h"
#include "hdfsEngine.h"

using namespace std;
using namespace faster;


TEST(testHDFSCreate, CreateEngine){
	faster::hdfsEngine fs;
	ASSERT_EQ(true, fs.isConnected())
		<< "Unable to Connect to HDFS";
	EXPECT_EQ(true, fs.isReady())
		<< "HDFS not ready";
}

class testHDFSFile: public ::testing::Test{
	public:
		faster::hdfsEngine fs;
};

TEST_F(testHDFSFile, ReadFile){
	string buffer;
	buffer.resize(128*1024);
	faster::hdfsFile file = fs.open("/tmp/test.txt", R);
	size_t n = file.read((char *) buffer.data(), buffer.size());
	ASSERT_EQ(12, n)
		<< "Read Wrong file size";
	EXPECT_STREQ("1\n2\n3\n4\n5\n7\n", buffer.data())
		<< "Read Wrong file data";
	fs.close(file);
}

TEST_F(testHDFSFile, WriteFile){
	string buffer = "abracadabra";

	// Write
	faster::hdfsFile file = fs.open("/tmp/testw.txt", CW);
	EXPECT_EQ(true, fs.exists("/tmp/testw.txt"))
		<< "File should exist";
	size_t n = file.write((char *) buffer.data(), buffer.size());
	file.close();

	// Read
	buffer.resize(128*1024);
	faster::hdfsFile fileR = fs.open("/tmp/testw.txt", R);
	n = fileR.read((char *) buffer.data(), buffer.size());
	ASSERT_EQ(11, n)
		<< "Read Wrong file size";
	buffer.resize(n);

	// Check Data
	EXPECT_STREQ("abracadabra", buffer.data())
		<< "Read Wrong file data";
	fileR.del();
	sleep(1);
	EXPECT_EQ(false, fs.exists("/tmp/testw.txt"))
		<< "File should be deleted";

}

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
