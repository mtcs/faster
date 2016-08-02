#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "libfaster.h"

using namespace std;
using namespace faster;

TEST(testFaster, ImplementTest){
	EXPECT_EQ(0, 1)
		<< "Test not implemented yet!!!";
}

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
