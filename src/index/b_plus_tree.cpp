#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * DONE: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  auto *index_roots = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));

  /* check if this index exsist */
  page_id_t root_p_id = INVALID_PAGE_ID;
  if (index_roots->GetRootId(index_id, &root_p_id)) {
    root_page_id_ = root_p_id;
  } else {
    /* create new empty entry */
    UpdateRootPageId(1); // insert <index_id_, root_page_id_>
    // index_roots->Insert(index_id, INVALID_PAGE_ID); // INVALID_PAGE at beginning, update when insert
  }
  
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/* call in IndexInfo Dtor 
 * delete <index_id, root_page_id> in INDEX_ROOTS_PAGE:
 * do this job in catalog manager
 */
void BPlusTree::Destroy(page_id_t current_page_id) {
  /* private members */
  if (current_page_id == INVALID_PAGE_ID) {
    return;
  }

  auto *notype_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  /* if this page is internal page, delete all subtrees */
  if (!notype_page->IsLeafPage()) {
    auto *itn_page = reinterpret_cast<InternalPage *>(notype_page);
    for (int i = 0; i < itn_page->GetSize(); ++i) {
      page_id_t child_p_id = itn_page->ValueAt(i);
      Destroy(child_p_id);
    }
  }

  /* delete this page: LeafPage and InternalPage */
  buffer_pool_manager_->DeletePage(current_page_id);
  return;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return (root_page_id_ == INVALID_PAGE_ID);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  /* find leaf page, using subroutine. */
  Page *page = FindLeafPage(key, root_page_id_);
  LeafPage *leaf = reinterpret_cast<LeafPage *> (page->GetData());
  RowId tmp_res = INVALID_ROWID; 

  bool key_exists = false;
  if (leaf->Lookup(key, tmp_res, processor_) ) {
    result.push_back(tmp_res);
    key_exists = true;
  } else {
    key_exists = false;
  }

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return key_exists;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  
  return InsertIntoLeaf(key, value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t id;
  auto *new_page = buffer_pool_manager_->NewPage(id);
  ASSERT(new_page != nullptr, "out of memory"); // exception

  auto *root_page = reinterpret_cast<BPlusTreeLeafPage *>(new_page->GetData());
  root_page->Init(id, INVALID_PAGE_ID, processor_.GetKeySize(), LEAF_PAGE_SIZE(processor_.GetKeySize()));
  root_page_id_ = id;
  UpdateRootPageId();

  InsertIntoLeaf(key, value);

  buffer_pool_manager_->UnpinPage(id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  ASSERT(!IsEmpty(), "Cannot insert into empty tree!");
  
  /* see whether insert key exist or not */
  std::vector<RowId> tmpRes;
  if (GetValue(key, tmpRes)) {
    // duplicate key, return false
    return false;
  }

  /* todo(Tao): insert operation, split... */
  /* 1. find LeafPage L */
  Page *page = FindLeafPage(key, root_page_id_);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  /* 2. if not full, insert to page */
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    leaf_page->Insert(key, value, processor_);
  } else {
  /* 3. if L is full, insert (temporarily) and split to L' */
    leaf_page->Insert(key, value, processor_);
    auto *new_leaf = Split(leaf_page, transaction);
    auto *new_key = new_leaf->KeyAt(0);
  /* 4. insert K'(smallest key of L') to parent */
    InsertIntoParent(leaf_page, new_key, new_leaf);
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);  
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  page_id_t parent_id = node->GetParentPageId();
  auto *page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(page != nullptr, "Out of memory.");

  auto *in_page = reinterpret_cast<InternalPage *>(page->GetData());
  in_page->Init(new_page_id, parent_id, node->GetKeySize(), INTERNAL_PAGE_SIZE(node->GetKeySize()));

  node->MoveHalfTo(in_page, buffer_pool_manager_);
  
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return in_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  page_id_t parent_id = node->GetParentPageId();
  auto *page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(page != nullptr, "Out of memory.");

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(new_page_id, parent_id, node->GetKeySize(), LEAF_PAGE_SIZE(node->GetKeySize()));

  node->MoveHalfTo(leaf_page);

  /* keep leaf nodes linked */
  leaf_page->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(leaf_page->GetPageId());
  
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  /* 1. if old_node is Root, create new root */
  if (old_node->IsRootPage()) {
    page_id_t old_p_id = old_node->GetPageId();
    page_id_t new_p_id = new_node->GetPageId();

    /* create new page */
    page_id_t new_root_p_id = INVALID_PAGE_ID;
    auto *new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(new_root_p_id));
    new_root->Init(new_root_p_id, INVALID_PAGE_ID, processor_.GetKeySize(), LEAF_PAGE_SIZE(processor_.GetKeySize()));

    /* populate new root page adopt this two */
    new_root->PopulateNewRoot(old_p_id, key, new_p_id);

    /* change this two's parent to new root */
    old_node->SetParentPageId(new_root_p_id);
    new_node->SetParentPageId(new_root_p_id);
    

    /* change root_page_id */
    root_page_id_ = new_root_p_id;
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(new_root_p_id, true);
    return;
  }
  Page *P = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(P->GetData());

  /* 2. let P = parent(old), if P is not full, insertAfter(old) */
  if (parent->GetSize() < parent->GetMaxSize()) {
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  } else if (parent->GetSize() == parent->GetMaxSize()) { 
  /* 3. if P is full, insertAfter(temporarily) and split; InsertIntoParent */
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    InternalPage *new_page = Split(parent, transaction);
    auto *new_key = new_page->KeyAt(0);
    InsertIntoParent(parent, new_key, new_page);
  } else {
    ASSERT(false, "size > max_size!!");
  }
  buffer_pool_manager_->UnpinPage(P->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if (IsEmpty())
    return;

  Page *page = FindLeafPage(key, root_page_id_);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  // /* ONLY FOR DEBUG */
  // int num_befor_delete = leaf_page->GetSize();
  // int num_after_delete = leaf_page->RemoveAndDeleteRecord(key, processor_);
  // bool delete_success = false;
  // delete_success = (num_after_delete + 1 == num_befor_delete);

  // if (!delete_success) {
  //   ASSERT(false, "Delete fail.");
  // }
  // /* ONLY FOR DEBUG(end) */
  leaf_page->RemoveAndDeleteRecord(key, processor_); // Comment when DEBUG


  if (leaf_page->IsRootPage()) {
    if (AdjustRoot(leaf_page)) {
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  } else if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    bool delete_leaf = CoalesceOrRedistribute(leaf_page, transaction);
    if (delete_leaf) {
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  } else {
    /* do nothing */
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/**
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  /* call bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) */
  ASSERT(!node->IsRootPage(), "Root page has no siblings.");

  /** Find sibling of input page **/
  /* get parent */
  Page *parent = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *inter_parent = reinterpret_cast<InternalPage *>(parent->GetData());

  int size_p = inter_parent->GetSize(); 
  page_id_t node_page_id = node->GetPageId();
  page_id_t sibling_page_id = INVALID_PAGE_ID;

  /* find the index in parent node of this page */
  int idx = 0;
  for (; idx < size_p; ++idx) {
    if (inter_parent->ValueAt(idx) == node_page_id) {
      break;
    }
  }
  ASSERT(idx != size_p, "Parent and child must point to each other.");
  ASSERT(size_p > 1, "Parent must have more than one child.");
  if (idx == 0) {
    /* sibling of first node is on right */
    sibling_page_id = inter_parent->ValueAt(idx + 1);
  } else {
    /* others' silibing on left */
    sibling_page_id = inter_parent->ValueAt(idx - 1);
  }

  /** decide Coalesce or Redistribute **/
  N *sibling_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_page_id)->GetData());
  int size1 = node->GetSize();
  int size2 = sibling_node->GetSize();

  bool delete_node = false;
  if (size1 + size2 <= node->GetMaxSize()) {
    /* Coalesce */
    delete_node = true;
    bool delete_parent = Coalesce(sibling_node, node, inter_parent, idx, transaction); // NOTE: node and sibling maybe swapped
    if (delete_parent) {
      buffer_pool_manager_->DeletePage(inter_parent->GetPageId());
    }
  } else {
    /* Redistribute */
    Redistribute(sibling_node, node, idx); // NOTE: node and sibling will not be swapped
    /* set parent's new middle key */
    int mid_idx = (idx == 0)? 1:idx;
    N *rhs_node = (idx == 0)? sibling_node:node;
    auto *rhs_leftmost_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, rhs_node->GetPageId(), true)->GetData());
    GenericKey *new_middle_key = rhs_leftmost_leaf->KeyAt(0);
    inter_parent->SetKeyAt(mid_idx, new_middle_key);
    delete_node = false;

    buffer_pool_manager_->UnpinPage(rhs_leftmost_leaf->GetPageId(), false);
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  return delete_node;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if (index == 0) {
    /* BEFORE SWAP: node(less) -> neighbor[delete]
     *              ^
     *              index = 0
     */
    LeafPage *tmp = node;
    node = neighbor_node;
    neighbor_node = tmp;
    index = 1;
    /* AFTER  SWAP: neighbor(less) -> node[delete] 
     *                                ^
     *                                index = 1
     */
  } else {
    /* no swap */
    /* neighbor -> node(less)[delete] 
     *             ^
     *             index != 0
     */
  }
  node->MoveAllTo(neighbor_node); // nodePage should be deleted outside this function

  /* delete pair in parent */
  bool delete_parent_node = false;
  parent->Remove(index);

  /* operations after delete item is parent */
  if (parent->IsRootPage()) {
    /* parent IS root page */
    if (AdjustRoot(parent)) {
      delete_parent_node = true;
    }
  } else {
    /* parent is NOT root page */
    if (parent->GetSize() < parent->GetMinSize()) {
      /* recursively call CorR */
      if (CoalesceOrRedistribute(parent, transaction)) {
        delete_parent_node = true;
      }
    }
  }
  
  return delete_parent_node;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if (index == 0) {
    /* BEFORE SWAP: node(less) -> neighbor[delete]
     *              ^
     *              index = 0
     */
    InternalPage *tmp = node;
    node = neighbor_node;
    neighbor_node = tmp;
    index = 1;
    /* AFTER  SWAP: neighbor(less) -> node[delete] 
     *                                ^
     *                                index = 1
     */
  } else {
    /* no swap */
    /* neighbor -> node(less)[delete] 
     *             ^
     *             index != 0
     */
  }
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);

  /* delete pair in parent */
  bool delete_parent_node = false;
  parent->Remove(index);

  /* operations after delete item is parent */
  if (parent->IsRootPage()) {
    /* parent IS root page */
    if (AdjustRoot(parent)) {
      delete_parent_node = true;
    }
  } else {
    /* parent is NOT root page */
    if (parent->GetSize() < parent->GetMinSize()) {
      /* recursively call CorR */
      if (CoalesceOrRedistribute(parent, transaction)) {
        delete_parent_node = true;
      }
    }
  }
  
  return delete_parent_node;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * The change in parent node's pair is not made in this function. 
 * 
 * NOTE!!!!!!!!!!!!!!!!!!!!
 * When call Redistribute(N*), set parent middle key the smallest of right node.
 * 
 * @param   neighbor_node      sibling page of input "node", for index != 0,
 *  neighbor is on left of node.
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  ASSERT(!node->IsRootPage(), "root page cannot redistribute.");

  if (index == 0) {
    /*  node(less) -> neighbor
     *  ^
     *  index = 0
     */
    neighbor_node->MoveFirstToEndOf(node);    
  } else {
    /*  neighbor -> node(less)
     *              ^
     *              index != 0
     */
    neighbor_node->MoveLastToFrontOf(node);
  }
  ASSERT(neighbor_node->GetSize() >= neighbor_node->GetMinSize(), "Underflow after redistribution.");
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * The change in parent node's pair is not made in this function. 
 * 
 * NOTE!!!!!!!!!!!!!!!!!!!!
 * When call Redistribute(N*), set parent middle key the smallest of right node.
 * 
 * @param   neighbor_node      sibling page of input "node", for index != 0,
 *  neighbor is on left of node.
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  ASSERT(!node->IsRootPage(), "root page cannot redistribute.");
  
  auto *inter_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    /*  node(less) -> neighbor
     *  ^
     *  index = 0
     */

    /* find middle key */
    GenericKey *middle = inter_root->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node, middle, buffer_pool_manager_);    
  } else {
    /*  neighbor -> node(less)
     *              ^
     *              index != 0
     */

    /* find middle key */
    GenericKey *middle = inter_root->KeyAt(index);
    neighbor_node->MoveLastToFrontOf(node, middle, buffer_pool_manager_);
  }

  ASSERT(neighbor_node->GetSize() >= neighbor_node->GetMinSize(), "Underflow after redistribution.");
  buffer_pool_manager_->UnpinPage(inter_root->GetPageId(), false);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  ASSERT(old_root_node->IsRootPage(), "old_root_node Must be RootPage.");

  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    /* case 1 */
    auto *internal_root = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t only_child_id = internal_root->RemoveAndReturnOnlyChild();
    root_page_id_ = only_child_id;
    UpdateRootPageId();

    /* new root's parent id is INVALID */
    auto *new_root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(only_child_id)->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(only_child_id, true);
  } else if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    /* case 2 */
    auto *leaf_root = reinterpret_cast<LeafPage *>(old_root_node);
    leaf_root->RemoveAndDeleteRecord(leaf_root->KeyAt(0),processor_);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
  } else {
    // ASSERT(false, "only case1 and case2 is valid.");
    return false;
  }

  /* delete old_root_page */
  // buffer_pool_manager_->DeletePage(old_root_node->GetPageId()); // do not delete inside this function
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  /* page will be unpinned in Dtor */
  Page *page = FindLeafPage(nullptr, root_page_id_, true); 
  page_id_t p_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(p_id, false); // unpin after FindLeafPage
  return IndexIterator(p_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  /* remember to use int LeafPage::KeyIndex() */
  Page *page = FindLeafPage(key, root_page_id_);
  page_id_t p_id = page->GetPageId();

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int idx = leaf_page->KeyIndex(key, processor_);

  buffer_pool_manager_->UnpinPage(p_id, false); // unpin after FindLeafPage
  return IndexIterator(p_id, buffer_pool_manager_, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, -1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Start form 'page_id'!!!!!
 * Note: the leaf page is pinned, you need to UNPIN!! it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  auto *begin_page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(begin_page->GetData());

  while (!cur_page->IsLeafPage()) {
    /* cur_page is InternalPage */
    auto *in_page = reinterpret_cast<InternalPage *>(cur_page);
    page_id_t next_pgae_id;
    if (leftMost) {
      next_pgae_id = in_page->ValueAt(0);
    } else {
      next_pgae_id = in_page->Lookup(key, processor_);
    }

    page_id_t this_page_id = in_page->GetPageId();

    cur_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(next_pgae_id)->GetData());
    buffer_pool_manager_->UnpinPage(this_page_id, false);
  }
  
  page_id_t ret_leaf_p_id = cur_page->GetPageId();
  buffer_pool_manager_->UnpinPage(ret_leaf_p_id, false);
  return buffer_pool_manager_->FetchPage(ret_leaf_p_id);
  /* remember to unpin after use */
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto *index_roots = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  
  if (insert_record) {
    index_roots->Insert(index_id_, root_page_id_);
  } else {
    /* update */
    index_roots->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      // out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
      out << "<TD>" << leaf->ValueAt(i).GetPageId() << "," << leaf->ValueAt(i).GetSlotNum() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        /**for debug: ONLY for type:INT **/
        std::vector<Column *> columns = {
            new Column("int", TypeId::kTypeInt, 0, false, false),
        };
        Schema *table_schema_ii = new Schema(columns);
        KeyManager KP(table_schema_ii, 16);

        // make row_key
        vector<Field> empty_filed;
        Row row_key = Row(empty_filed);

        //get stored key
        GenericKey *key = inner->KeyAt(i);
        KP.DeserializeToKey(key, row_key, table_schema_ii);

        string str_int_key = row_key.GetField(0)->toString();

        out << str_int_key;
        /**(end)for debug: ONLY for type:INT **/
        // out << inner->KeyAt(i); // comment when debug
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}