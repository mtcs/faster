#include <fstream>

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
	faster::hdfsFile file = fs.open("/tmp/testhdfs.txt", R);
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
	faster::hdfsFile file = fs.open("/tmp/testhdfsw.txt", CW);
	EXPECT_EQ(true, fs.exists("/tmp/testhdfsw.txt"))
		<< "File should exist";
	size_t n = file.write((char *) buffer.data(), buffer.size());
	file.close();

	// Read
	buffer.resize(128*1024);
	faster::hdfsFile fileR = fs.open("/tmp/testhdfsw.txt", R);
	n = fileR.read((char *) buffer.data(), buffer.size());
	ASSERT_EQ(11, n)
		<< "Read Wrong file size";
	buffer.resize(n);

	// Check Data
	EXPECT_STREQ("abracadabra", buffer.data())
		<< "Read Wrong file data";
	fileR.del();
	sleep(1);
	EXPECT_EQ(false, fs.exists("/tmp/testhdfsw.txt"))
		<< "File should be deleted";

}
TEST_F(testHDFSFile, GetLine){
	string hdfsLine;
	string realLine;
	int linen = 0;

	// Open Files
	std::ifstream rfile("../../res/soc-Epinions1-el.txt", std::ifstream::in);
	faster::hdfsFile file = fs.open("/tmp/soc-Epinions1-el.txt", R);
	ASSERT_EQ(true, rfile.good());
	ASSERT_EQ(true, file.good());

	while (rfile.good()) {
		hdfsLine = file.getLine('\n');
		std::getline(rfile, realLine);

		ASSERT_STREQ(realLine.data(), hdfsLine.data()) << "Error reading line " << linen << " "
			<< hdfsLine << " != " << realLine ;
		linen++;
	}

	ASSERT_EQ(false, rfile.good());
	ASSERT_EQ(false, file.good());

}
TEST_F(testHDFSFile, GetLineOffset){
	string hdfsLine;
	string realLine;
	int linen = 0;

	// Open Files
	std::ifstream rfile("../../res/soc-Epinions1-el.txt", std::ifstream::in);
	faster::hdfsFile file = fs.open("/tmp/soc-Epinions1-el.txt", R);
	ASSERT_EQ(true, rfile.good());
	ASSERT_EQ(true, file.good());

	hdfsLine = file.getLine(16*1024, '\n');
	rfile.seekg(16*1024);
	std::getline(rfile, realLine);

	while (rfile.good()) {
		hdfsLine = file.getLine('\n');
		std::getline(rfile, realLine);

		ASSERT_STREQ(realLine.data(), hdfsLine.data()) << "Error reading line " << linen << " "
			<< hdfsLine << " != " << realLine ;
		linen++;
	}

	ASSERT_EQ(false, rfile.good());
	ASSERT_EQ(false, file.good());

}
TEST_F(testHDFSFile, FileBlocks){
	faster::hdfsFile file = fs.open("/tmp/soc-Epinions1-el.txt", R);
	std::vector<std::deque<int>>  loc = file.getBlocksLocations();
	EXPECT_LT(0, loc.size());

	for ( size_t i = 0; i < loc.size() ; i++){
		EXPECT_LT(0, loc[i].size());
		std::cerr << "   (" << i << ") ";
		for (auto it = loc[i].begin() ; it != loc[i].end(); it++){
			std::cerr << *it << " ";
		}
		std::cerr << "\n";
	}
	file.close();
}

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
