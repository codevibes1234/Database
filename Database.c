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

const uint32_t ID_SIZE = sizeof(uint32_t);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_SIZE = 32;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_SIZE = 255;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;

void print_prompt() { printf("db > "); }

void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *cursor_value(Cursor *cursor) {
  Table *table = cursor->table;
  uint32_t page_num = cursor->page_num;
  void *node = get_page(table->pager, page_num);
  return leaf_node_value(node, cursor->cell_num);
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) {
  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  int status = pwrite(pager->file_descriptor, pager->pages[page_num],
                      (size_t)size, offset);
  if (status == -1)
    printf("Error while flushing pages\n");
  if (status == 0)
    printf("Reached the end of file\n");
  free(pager->pages[page_num]);
  pager->pages[page_num] = NULL;
}

uint32_t get_unused_page_num(Pager *pager) { return pager->num_of_pages; }

void db_close(Table *table) {
  Pager *pager = table->pager;
  for (uint32_t i = 0; i < pager->num_of_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int res = close(pager->file_descriptor);
  if (res == -1) {
    printf("Error closing db file\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->pages[i]) {
      free(pager->pages[i]);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  if (!strcmp((input_buffer->buffer), ".exit")) {
    close_input_buffer(&input_buffer);
    db_close(table);
    return META_COMMAND_SUCCESS;
  } else if (!strcmp((input_buffer->buffer), ".exit")) {
    printf("B* Tree : \n");
    print_tree(table->pager, table->root_page_num, 0);
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  int count = 0;
  char *string = input_buffer->buffer;
  char *token = strtok(string, " ");
  char insert[7] = "insert";
  char select[7] = "select";
  if (strcmp(token, insert) == 0) {
    statement->type = STATEMENT_INSERT;
    while (token != NULL) {
      count++;

      token = strtok(NULL, " ");
      if (token != NULL) {
        if (count == 1) {
          char *ptr;
          int num = strtol(token, &ptr, 10);
          if (num >= 0) {
            statement->row_to_insert.id = (uint32_t)num;
          } else {
            return PREPARE_NEGATIVE_ID;
          }
        } else if (count == 2) {
          if (strlen(token) > COLUMN_USERNAME_SIZE)
            return PREPARE_STRING_TOO_LONG;
          else
            strcpy(statement->row_to_insert.username, token);
        } else if (count == 3) {
          if (strlen(token) > COLUMN_EMAIL_SIZE)
            return PREPARE_STRING_TOO_LONG;
          else
            strcpy(statement->row_to_insert.email, token);
        } else {
          return PREPARE_SYNTAX_ERROR;
        }
      }
    }
    if (count < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  } else if (strcmp(token, select) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  } else {
    return PREPARE_UNRECOGNIZED_STATEMENT;
  }
}

Pager *pager_open(const char *filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }
  Pager *pager = (Pager *)calloc(1, sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = lseek(fd, 0, SEEK_END);
  pager->num_of_pages = (pager->file_length) / PAGE_SIZE;

  if (pager->file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }
  return pager;
}

Table *db_open(const char *filename) {
  Table *table = (Table *)calloc(1, sizeof(Table));
  table->pager = pager_open(filename);
  Pager *pager = table->pager;
  void *root_node = get_page(pager, 0);
  set_node_root(root_node, true);
  return table;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
  void *node = get_page(table->pager, table->root_page_num);
  if ((*leaf_node_num_cells(node)) >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }
  Row *row_to_insert = &(statement->row_to_insert);
  uint32_t key = statement->row_to_insert.id;
  Cursor *cursor = table_find(table, key);
  if (key == *(leaf_node_key(node, cursor->cell_num))) {
    return EXECUTE_DUPLICATE_KEY;
  }
  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
  free(cursor);
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
  if (statement->type == STATEMENT_INSERT) {
    return execute_insert(statement, table);
  } else if (statement->type == STATEMENT_SELECT) {
    Row row;
    Cursor *cursor = table_start(table);
    while (!(cursor->end_of_table)) {
      deserialize_row(cursor_value(cursor), &row);
      printf("(%d , %s , %s)\n", row.id, row.username, row.email);
      cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
  }
  return EXECUTE_FAIL;
}

InputBuffer *new_input_buffer() {
  InputBuffer *input_buffer = (InputBuffer *)calloc(1, sizeof(InputBuffer));
  return input_buffer;
}

ReadInputStatus read_input(InputBuffer *input_buffer) {

  input_buffer->input_length =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
  if (input_buffer->input_length == -1) {
    puts("ERROR WHILE GETTING INPUT (GETLINE)");
    return BUFFER_NOT_CREATED;
  } else {
    input_buffer->buffer[input_buffer->input_length - 1] = '\0';
    return BUFFER_CREATED;
  }
}

void close_input_buffer(InputBuffer **input_buffer) {
  free((*input_buffer)->buffer);
  free((*input_buffer));
  *input_buffer = NULL;
}

void *get_page(Pager *pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch pages out of bound\n");
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    void *page = calloc(1, PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    if (pager->file_length % PAGE_SIZE) {
      num_pages++;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file\n");
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
    if (page_num >= pager->num_of_pages) {
      pager->num_of_pages = page_num + 1;
    }
  }
  return pager->pages[page_num];
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Give a database filename.\n");
    exit(EXIT_FAILURE);
  }
  char *filename = argv[1];
  Table *table = db_open(filename);

  InputBuffer *input_buffer = new_input_buffer();
  if (input_buffer == NULL || table == NULL) {
    printf("Memory allocation failed.\n");
    exit(EXIT_FAILURE);
  }
  while (true) {
    print_prompt();
    ReadInputStatus status = read_input(input_buffer);
    if (status == BUFFER_NOT_CREATED) {
      printf("Error reading input\n");
      continue;
    }
    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
      case META_COMMAND_SUCCESS:
        exit(EXECUTE_SUCCESS);
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
        continue;
      }
    }
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_NEGATIVE_ID:
      printf("ID must be positive\n");
      continue;
    case PREPARE_STRING_TOO_LONG:
      printf("String is too long\n");
      continue;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error\n");
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword '%s'.\n", input_buffer->buffer);
      continue;
    }
    switch (execute_statement(&statement, table)) {
    case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Table full.\n");
      break;
    case EXECUTE_DUPLICATE_KEY:
      printf("Duplicate key.\n");
      break;
    case EXECUTE_FAIL:
      printf("Execution failed.\n");
      break;
    }
  }
  return 0;
}
