#include <iostream>
#include "testutils.h"

TEST(Jebtest, equal) {
    EXPECT_EQ(1, 1);
}

TEST(Jebtest, notequal) {
    EXPECT_EQ(1, 2);
}


class FooTest : public ::testing::Test {
  protected:
    // void SetUp() override { std::cout << "running setup\n"; }
    // void TearDown() override { std::cout << "running teardown\n"; }
};
TEST_F(FooTest, equal) {
    EXPECT_EQ(1, 1);
}


