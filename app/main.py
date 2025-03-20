import sys

from .sqlite import SQLiteDatabase
from .parse_util import basic_parse_sql


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
            columns, count_rows, table_name = basic_parse_sql(sql)

            result_iterator = database.query(
                table_name.value,
                selected_columns=columns,
                count_rows=count_rows,
            )

            for result in result_iterator:
                if type(result) is int:
                    print(result)
                elif type(result) is list:
                    print(", ".join(result))
