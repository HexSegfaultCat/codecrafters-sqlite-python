import sys

# import sqlparse
from .sqlite import SQLiteDatabase


database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with SQLiteDatabase(database_file_path) as database:
        print(f"database page size: {database.page_size}")

        tables = list(database.tables())
        print(f"number of tables: {len(tables)}")
else:
    print(f"Invalid command: {command}")
