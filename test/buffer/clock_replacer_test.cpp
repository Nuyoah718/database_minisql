#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

TEST(ClockReplacerTest, SampleTest) {
  CLOCKReplacer clock_replacer(7);

  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1);
  EXPECT_EQ(6, clock_replacer.Size());

  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  clock_replacer.Pin(3);
  clock_replacer.Pin(4);
  EXPECT_EQ(2, clock_replacer.Size());

  clock_replacer.Unpin(4);

  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

TEST(ClockReplacerTest, CornerCaseTest) {
  CLOCKReplacer clock_replacer(4);
  int value;
  bool result = clock_replacer.Victim(&value);
  EXPECT_FALSE(result);

  clock_replacer.Unpin(3);
  clock_replacer.Unpin(2);
  EXPECT_EQ(2, clock_replacer.Size());
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Unpin(1);
  EXPECT_EQ(2, clock_replacer.Size());
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, clock_replacer.Size());
}
