#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

/* remember to call: int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) */
IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  if (item_index < page->GetSize() - 1) {
    item_index++;
  } else if (page->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t old_page_id = current_page_id;
    current_page_id = page->GetNextPageId();
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());

    /* first on next index */
    item_index = 0;
    buffer_pool_manager->UnpinPage(old_page_id, false);
  } else if (page->GetNextPageId() == INVALID_PAGE_ID) {
    /* change to index end() */
    buffer_pool_manager->UnpinPage(current_page_id, false);
    current_page_id = INVALID_PAGE_ID;
    page = nullptr;
    item_index = -1;
  } else {
    ASSERT(false, "Can't reach here.");
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}