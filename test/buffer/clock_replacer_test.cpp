#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

class ClockReplacerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    clock_replacer_ = std::make_unique<CLOCKReplacer>(7);
  }

  void TearDown() override {
    clock_replacer_.reset();
  }

  std::unique_ptr<CLOCKReplacer> clock_replacer_;
};

TEST_F(ClockReplacerTest, SampleTest) {
  for (int i = 1; i <= 6; ++i) {
    clock_replacer_->Unpin(i);
  }
  EXPECT_EQ(6, clock_replacer_->Size());

  int value;
  for (int expected = 1; expected <= 3; ++expected) {
    clock_replacer_->Victim(&value);
    EXPECT_EQ(expected, value);
  }

  clock_replacer_->Pin(3);
  clock_replacer_->Pin(4);
  EXPECT_EQ(2, clock_replacer_->Size());

  clock_replacer_->Unpin(4);

  for (int expected : {5, 6, 4}) {
    clock_replacer_->Victim(&value);
    EXPECT_EQ(expected, value);
  }
}

TEST_F(ClockReplacerTest, CornerCaseTest) {
  clock_replacer_->Unpin(3);
  clock_replacer_->Unpin(2);
  EXPECT_EQ(2, clock_replacer_->Size());

  int value;
  clock_replacer_->Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer_->Unpin(1);
  EXPECT_EQ(2, clock_replacer_->Size());

  for (int expected : {3, 1}) {
    clock_replacer_->Victim(&value);
    EXPECT_EQ(expected, value);
  }

  EXPECT_FALSE(clock_replacer_->Victim(&value));
  EXPECT_EQ(0, clock_replacer_->Size());
}
