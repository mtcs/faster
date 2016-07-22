#include "gtest/gtest.h"
#include "hdfsEngine.h"

using namespace std;
using namespace faster;


TEST(testHDFSCreate, CreateEngine){
	faster::hdfsEngine fs;
	EXPECT_EQ(true, fs.isReady());
}

class testHDFSFile: public ::testing::Test{
	public:
		faster::hdfsEngine fs;
};

TEST_F(testHDFSFile, CreateROFile){
	faster::hdfsFile file = fs.open("/tmp/teste.txt", W);
	fs.close(file);
}

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
