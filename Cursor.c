#include "Database.h"
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

Cursor *table_start(Table *table) {
  Cursor *cursor = table_find(table, 0);
  void *node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);
  return cursor;
}

void cursor_advance(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *node = get_page(cursor->table->pager, page_num);
  cursor->cell_num++;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

Cursor *table_find(Table *table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void *root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
    exit(EXIT_FAILURE);
  }
}

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {

  Cursor *cursor = calloc(1, sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;

  void *node = get_page(table->pager, page_num);
  uint32_t num_cells = *(leaf_node_num_cells(node));

  uint32_t min_val = 0;
  uint32_t max_val = num_cells;
  while (max_val != min_val) {
    uint32_t index = (min_val + max_val) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    } else if (key > key_at_index) {
      max_val = index;
    } else {
      min_val = index + 1;
    }
  }
  cursor->cell_num = min_val;
  return cursor;
}

Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page(table->pager, page_num);
  uint32_t num_keys = *internal_node_num_keys(node);

  uint32_t index = internal_node_find_child(node, key);

  uint32_t next_node_page_num = *internal_node_child(node, index);
  void *child = get_page(table->pager, next_node_page_num);
  if (get_node_type(child) == NODE_LEAF) {
    return leaf_node_find(table, next_node_page_num, key);
  } else {
    return internal_node_find(table, next_node_page_num, key);
  }
}

uint32_t internal_node_find_child(void *node, uint32_t key) {
  uint32_t num_keys = *internal_node_num_keys(node);

  uint32_t min_val = 0;
  uint32_t max_val = num_keys;
  while (max_val != min_val) {
    uint32_t index = (min_val + max_val) / 2;
    uint32_t key_at_index = *internal_node_key(node, index);
    if (key <= key_at_index) {
      max_val = index;
    } else {
      min_val = index + 1;
    }
  }
  return min_val;
}