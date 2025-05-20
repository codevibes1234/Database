# Database using B* trees

This project is a depiction of how real-life databases work using B* trees. The aim of the project is to 
make an efficient data keeping and tracking database with efficient insert and search operations.

## Command to use it

You can use any desirable compiler. We have used `gcc-14` for example sake.

`gcc-14 -o Database.out Database.c Node.c Cursor.c`

`./Database.out <Database db file>`

## Table and Pager

The table is written to and read from `Databse.db`. Since the table is huge, it is divided into pages.
`Pager` is responsible for handling extracting pages into the cache. Once the operations are done, 
the pages extracted by `Pager` are written back to hard memory i.e. `Database.db`

## Cursor

`Cursor` is responsible for returning the current row we are at. Each page has multiple rows and the 
cursor shows us the following properties : 
- Current `page_num`
- Current `cell_num` which is the same as the number of row in that page
- Whether we are at the end of table or not

## Nodes and B* trees

Structurally, B* trees have leaf nodes and internal nodes. For a classic B tree, both leaf and internal 
nodes store the key-value pair, but in case of B* trees, internal nodes help navigating to relevant 
leaf node and it is the leaf nodes that store the rows extracted from database.

In the project implementation, both internal node and leaf node are of the same size but function differently.
While internal nodes store key and corresponding page numbers that navigate to leaf nodes, leaf nodes store 
key and value(value is the row in this context).

Time complexity of both search and insert is `O(logn)`

## TODO list 
- Fix minor bugs
- Add build system
- Add delete row operations