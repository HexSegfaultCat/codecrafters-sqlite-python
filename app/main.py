import sys

from .sqlite import SQLiteDatabase


database_file_path = sys.argv[1]
command = sys.argv[2]

with SQLiteDatabase(database_file_path) as database:
    match (command[0] == "."), command:
        case True, ".dbinfo":
            db_header = database.header()
            schema_tables = list(database.schema_tables())

            print(f"database page size: {db_header.page_size}")
            print(f"number of tables: {len(schema_tables)}")

        case True, ".tables":
            table_names = sorted(
                [schema_table.tbl_name for schema_table in database.schema_tables()]
            )
            print(" ".join(table_names))

        case True, _:
            print(f"Invalid command: {command}")

        case False, sql:
            statements = sql.split(" ")

            assert len(statements) == 4
            assert statements[0].upper() == "SELECT"
            assert statements[1].upper() == "COUNT(*)"
            assert statements[2].upper() == "FROM"

            table_name = statements[3]

            rows_count = database.total_row_count(table_name)
            print(rows_count)
