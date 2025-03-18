import sys

# import sqlparse
from .sqlite import SQLiteDatabase


database_file_path = sys.argv[1]
command = sys.argv[2]

with SQLiteDatabase(database_file_path) as database:
    match command:
        case ".dbinfo":
            header = database.header()
            print(f"database page size: {header.page_size}")

            tables = list(database.schema_tables())
            print(f"number of tables: {len(tables)}")

        case ".tables":
            table_names = sorted(
                [schema_table.tbl_name for schema_table in database.schema_tables()]
            )
            print(" ".join(table_names))

        case _:
            print(f"Invalid command: {command}")
