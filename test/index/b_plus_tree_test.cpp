#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 16);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 3000; // change test size
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  // vector<int> delete_seq_N;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
    // delete_seq_N.push_back(i);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);

  // /* DEBUG */
  // // Shuffle data
  // int seed1 = std::chrono::system_clock::now().time_since_epoch().count();
  // int seed2 = seed1 + 233;    // random but different from seed1
  // cout << "seed1: " << seed1 << endl;
  // cout << "seed2: " << seed2 << endl;
  // // int seed1 = -1876133983; // when there is a bug, set to seed that cause bug.
  // // int seed2 = -1876133983;
  // shuffle(keys.begin(), keys.end(), default_random_engine(seed1));
  // shuffle(values.begin(), values.end(), default_random_engine(seed1));
  // shuffle(delete_seq.begin(), delete_seq.end(), default_random_engine(seed2));
  // shuffle(delete_seq_N.begin(), delete_seq_N.end(), default_random_engine(seed2));

  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  // tree.PrintTree(mgr[0]);
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);

    // /* show delete seq */
    // cout << "delete:" << delete_seq_N[i] << "  cnt = " << i << endl;

    /* print tree */
    // tree.PrintTree(mgr[i]);
  }
  // tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}