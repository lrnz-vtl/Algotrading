#include <gtest/gtest.h>
#include "series.h"

TEST(DummyTest, DummyTestCase)
{
    EXPECT_EQ(1000, 10*10*10);
}

int main() {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}