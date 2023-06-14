#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

/* Q: (Tao Chengjian)
 *    Todo: determine
 *    [zju_db] pair_size = (GetKeySize() + sizeof(RowId))
 *    or 
 *    pair_size = (sizeof(std::pair<GenericKey *, RowId>) )
 * A: Since the <key, Rowid> is strored in page, so must serialize to page!
 *    Therefore, we don't store pair<GenericKey *, RowId>, which is a pointer 
 *    in memory.
 */
#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  /* set meta data */
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * DONE: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  if (!KM.CompareKeys(key, KeyAt(0)) > 0) {
    /* key <= A[0] */
    return 0;
  }

  /* find in range [l, r), where A[l] < key <= A[r] */
  int l = 0, r = GetSize(), m = l + (r - l) / 2;
  while (l + 1 != r) {
    int res = KM.CompareKeys(key, KeyAt(m));
    if (res > 0) {
      /* key > A[m] */
      l = m;
    } else {
      /* key <= A[m] */
      r = m;
    }
    m = l + (r - l) / 2;
  }

  return r;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * pair_size);
}

void LeafPage::PairMove(void *dest, void *src, int pair_num) {
  memmove(dest, src, pair_num * pair_size);
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair<GenericKey *, RowId>(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int size = GetSize();
  /* Maybe we can use ASSERT(size <= GetMaxSize()) for 'TEMPORARY OVERFLOW' */
  ASSERT(size <= GetMaxSize(), "LeafPage is full, cannot Insert.");

  // Linear scan: O(N)
  int idx = 0;
  for (; idx < size; ++idx) {
    int res = KM.CompareKeys(key, KeyAt(idx));
    ASSERT(res != 0, "Duplicated keys.");
    if (res < 0) {
      /* key > KeyAt(ind - 1) && key < KeyAt(ind) */
      break;
    }
  }

  // Move and insert: O(N)
  if (idx < size) {
    // PairMove: O(N) to make space for insersion
    int num = size - idx;
    PairMove(PairPtrAt(idx + 1), PairPtrAt(idx), num);
  }

  // insert O(1)
  SetKeyAt(idx, key);
  SetValueAt(idx, value);
  // size count increase
  SetSize(size + 1);

  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize();
  int half_size = size / 2;

  /* copy pair[half_size]~pair[size - 1] to recipient */
  recipient->CopyNFrom(PairPtrAt(half_size), size - half_size);

  SetSize(half_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  ASSERT(GetSize() == 0, "Recipient should be empty.");
  /* copy from src to this InternalPage */
  PairCopy(PairPtrAt(0), src, size);
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int size = GetSize();
  /* Linear scan: O(N) */
  for (int i = 0; i < size; ++i) {
    if (KM.CompareKeys(key, KeyAt(i)) == 0) {
      value = ValueAt(i);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  /* Find key to delete: O(N) */
  int size = GetSize();
  ASSERT(size > 0, "Conot delete key in an empty leaf.");
  
  int index = 0;
  for (; index < size; ++index) {
    if (KM.CompareKeys(key, KeyAt(index)) == 0) {
      break;
    }
  }

  if (index == size) {
    /* key is not found */
    return size;
  } else if (index < size - 1) {
    /* perform deletion: O(N) */
    int num = size - index - 1;
    PairMove(PairPtrAt(index), PairPtrAt(index + 1), num);
  } else if (index == size - 1) {
    /* do nothing */
  }

  /* We don't consider the underflow here, 
   * any new size include 0 is OK.
   */
  SetSize(size - 1);
  return size - 1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  /* check recipient on left of this LeafPage 
   * : recipient -> this -> rightPage 
   */
  ASSERT(recipient->GetNextPageId() == GetPageId(), "Recipient should be preious page.");
  
  /* check sum of size */
  int recip_size = recipient->GetSize();
  int size = GetSize();
  int sum_size = size + recip_size;
  ASSERT(sum_size <= GetMaxSize(), "This merge will cause overflow.");

  PairCopy(recipient->PairPtrAt(recip_size), PairPtrAt(0), size);
  recipient->SetNextPageId(GetNextPageId());

  /* modify size */
  recipient->SetSize(sum_size);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  /* check: recipient -> this */
  ASSERT(recipient->GetNextPageId() == GetPageId(), "Should be recipient -> this.");
  /* check recipient is not full */
  ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "recipient is full.");
  ASSERT(GetSize() > 0, "Empty leaf.");

  /* move first to recipient's end */
  int size = GetSize();
  int r_size = recipient->GetSize();
  ASSERT(size > 1, "Must have item for recipient.");

  /* copyLast from this node */
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  ASSERT(r_size + 1 == recipient->GetSize() 
      && r_size + 1 >= recipient->GetMinSize(), "Recipient still underflow.");

  /* delete first element in this node */
  PairMove(PairPtrAt(0), PairPtrAt(1), size - 1);

  /* modify size */
  SetSize(size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  /* check page is not full */
  int size = GetSize();
  ASSERT(size < GetMaxSize(), "LeafPage is full.");
  
  SetKeyAt(size, key);
  SetValueAt(size, value);
  SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  /* check: this -> recipient */
  ASSERT(GetNextPageId() == recipient->GetPageId(), "Should be this -> recipient.");
  /* check recipient is not full */
  ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "recipient is full.");
  ASSERT(GetSize() > 0, "Empty leaf.");

  /* move Last to recipient's front */
  int size = GetSize();
  int r_size = recipient->GetSize();
  /* copyFirst from this node */
  recipient->CopyFirstFrom(KeyAt(size - 1), ValueAt(size - 1));

  /* modify size */
  SetSize(size - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  /* check page is not full */
  int size = GetSize();
  ASSERT(size < GetMaxSize(), "LeafPage is full.");
  
  PairMove(PairPtrAt(1), PairPtrAt(0), size);
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(size + 1);
}