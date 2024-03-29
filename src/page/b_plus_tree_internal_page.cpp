#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

/* Q: (Tao Chengjian)
 *    Todo: determine
 *    [zju_db] pair_size = (GetKeySize() + sizeof(page_id_t))
 *    or 
 *    pair_size = (sizeof(std::pair<GenericKey *, page_id_t>) )
 * A: Since the <key, page_id_t> is strored in page, so must serialize to page!
 *    Therefore, we don't store pair<GenericKey *, RowId>, which is a pointer 
 *    in memory.
 */
#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  /* set meta data */
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * pair_size);
}

void InternalPage::PairMove(void *dest, void *src, int pair_num) {
  memmove(dest, src, pair_num * pair_size);
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
  if (size < 2) {
    return INVALID_PAGE_ID;
  }

  if (KM.CompareKeys(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }

  // binary search, [l, r)
  int l = 1, r = size, m = l + (r - l) / 2;
  while (l < r - 1) {
    if (KM.CompareKeys(key, KeyAt(m)) < 0) {
      r = m;
    } else {
      l = m;
    }
    m = l + (r - l) / 2;
  }

  // key[l] < key, Key[l + 1] == Key[r] >= key
  return ValueAt(l);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  ASSERT(GetSize() == 0, "New root should be empty.");
  SetValueAt(0,old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);

  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int size = GetSize();
  ASSERT(size >= 2 && size <= GetMaxSize(), "InternalPage is full, cannot Insert.");

  // ValueIndex: O(N) to find index
  int old_ind = ValueIndex(old_value);
  if (old_ind == -1) {
    ASSERT(false, "No such old_value.");
  }

  if (old_ind < size - 1) {
    // PairMove: O(N) to make space for insersion
    int num = size - old_ind - 1;
    PairMove(PairPtrAt(old_ind + 2), PairPtrAt(old_ind + 1), num);
  } else if (old_ind == size -1) {
    // insert to tail
    // do nothing here
  } else {
    ASSERT(false, "old_ind == size. Impossible.");
  }
  // insert O(1)
  SetKeyAt(old_ind + 1, new_key);
  SetValueAt(old_ind + 1, new_value);

  // size count increase
  SetSize(size + 1);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int half_size = size / 2;

  /* copy pair[half_size]~pair[size - 1] to recipient */
  recipient->CopyNFrom(PairPtrAt(half_size), size - half_size, buffer_pool_manager);

  SetSize(half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  ASSERT(GetSize() == 0, "Recipient should be empty.");
  /* copy from src to this InternalPage */
  PairCopy(PairPtrAt(0), src, size);

  /* change children's parent to this page */
  for (int i = 0; i < size; ++i) {
    page_id_t child_page_id = ValueAt(i);
    auto *page = buffer_pool_manager->FetchPage(child_page_id);
    auto *notype_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // set child's parentId to this pageId.
    notype_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  /* delete element in array: O(N) */
  int size = GetSize();
  ASSERT(index >= 0 && index < size, "index not in [0, num_pairs)");

  if (index < size - 1) {
    /* move the follwing pairs forward */
    int num = size - 1 - index;
    PairMove(PairPtrAt(index), PairPtrAt(index + 1), num);
  } else if (index == size - 1) {
    // do nothing, the last pair.
  }

  SetSize(size - 1); 
  /* pending: delete the Page itself if the page becomes empty? */
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  ASSERT(IsRootPage() && !IsLeafPage(), "PageType not satisfied.");
  ASSERT(GetSize() == 1, "More than one child.");

  page_id_t only_child_id = ValueAt(0);
  SetSize(0); // delete last child
  return only_child_id;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  /* recipient -> this -> rightPage */
  /* check sum of size */
  int recip_size = recipient->GetSize();
  int size = GetSize();
  int sum_size = size + recip_size;
  ASSERT(sum_size <= GetMaxSize(), "This merge will cause overflow.");

  PairCopy(recipient->PairPtrAt(recip_size), PairPtrAt(0), size);
  recipient->SetKeyAt(recip_size, middle_key);

  /* change children's parent to recipient */
  for (int i = recip_size; i < sum_size; ++i) {
    page_id_t child_page_id = recipient->ValueAt(i);
    auto *page = buffer_pool_manager->FetchPage(child_page_id);
    auto *notype_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // set child's parentId to recipient's pageId.
    notype_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }

  /* modify size */
  recipient->SetSize(sum_size);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  /* this function is used in case: recipient -> this */
  /* check recipient is not full */
  ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "recipient is full.");
  ASSERT(GetSize() > 0, "This page is empty.");

  /* move Last to recipient's front */
  int size = GetSize();
  int r_size = recipient->GetSize();
  /* ...||r.last_key|r.last_pointer||middle_key|this.first_pointer|| */
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);

  /* modify size */
  Remove(0);
  SetSize(size - 1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  /* check page is not full */
  int size = GetSize();
  ASSERT(size < GetMaxSize(), "LeafPage is full.");
  
  SetKeyAt(size, key);
  SetValueAt(size, value);

  /* 'adopt' this page */
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(GetPageId());
  
  SetSize(size + 1);
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  /* this function is used in case: this -> recipient */
  /* check recipient is not full */
  ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "recipient is full.");
  ASSERT(GetSize() > 0, "This page is empty.");

  /* move Last to recipient's front */
  int size = GetSize();
  int r_size = recipient->GetSize();
  /* copyLast from this node */
  recipient->CopyFirstFrom(ValueAt(size - 1), buffer_pool_manager);
  /* ||dummy_key|this.last_pointer||middle_key|r.first_pointer||... */
  recipient->SetKeyAt(1, middle_key); 

  /* modify size */
  SetSize(size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  /* check page is not full */
  int size = GetSize();
  ASSERT(size < GetMaxSize(), "LeafPage is full.");
  
  PairMove(PairPtrAt(1), PairPtrAt(0), size);
  SetValueAt(0, value);

  /* 'adopt' this page */
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(GetPageId());
  
  SetSize(size + 1);
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}