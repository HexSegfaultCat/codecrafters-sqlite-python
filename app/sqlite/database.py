from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, cast, final

import re

from os import fstat, PathLike

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, Token
from sqlparse.tokens import Literal

from .schema import SchemaObject
from .record import Record, SerialType, parse_records
from .cell import (
    IndexBTreeInteriorCell,
    IndexBTreeLeafCell,
    TableBTreeInteriorCell,
    TableBTreeLeafCell,
)
from .page import BTreePage, OverflowPage, PageType
from .utils import BytesOffsetArray, OffsetMetadata


@final
class HeaderOffset:
    SQLITE_MAGIC_STRING = b"SQLite format 3\x00"

    # INFO: https://www.sqlite.org/fileformat.html#the_database_header
    HEADER_STRING = OffsetMetadata(OFFSET=0, SIZE=16)
    PAGE_SIZE = OffsetMetadata(OFFSET=16, SIZE=2)
    FILE_WRITE_FORMAT = OffsetMetadata(OFFSET=18, SIZE=1)
    FILE_READ_FORMAT = OffsetMetadata(OFFSET=19, SIZE=1)
    PAGE_RESERVED_BYTES = OffsetMetadata(OFFSET=20, SIZE=1)
    MAX_EMBEDDED_PAYLOAD_FRACTION = OffsetMetadata(OFFSET=21, SIZE=1)
    MIN_EMBEDDED_PAYLOAD_FRACTION = OffsetMetadata(OFFSET=22, SIZE=1)
    LEAF_PAYLOAD_FRACTION = OffsetMetadata(OFFSET=23, SIZE=1)
    FILE_CHANGE_COUNTER = OffsetMetadata(OFFSET=24, SIZE=4)
    FILE_SIZE_IN_PAGES = OffsetMetadata(OFFSET=28, SIZE=4)
    FIRST_FREELIST_TRUNK_PAGE_NUMBER = OffsetMetadata(OFFSET=32, SIZE=4)
    TOTAL_FREELIST_PAGES = OffsetMetadata(OFFSET=36, SIZE=4)
    SCHEMA_COOKIE = OffsetMetadata(OFFSET=40, SIZE=4)
    SCHEMA_FORMAT_NUMBER = OffsetMetadata(OFFSET=44, SIZE=4)
    DEFAULT_PAGE_CACHE_SIZE = OffsetMetadata(OFFSET=48, SIZE=4)
    LARGEST_BTREE_ROOT_PAGE_NUMBER = OffsetMetadata(OFFSET=52, SIZE=4)
    DATABASE_TEXT_ENCODING = OffsetMetadata(OFFSET=56, SIZE=4)
    # TODO: Add the missing ones


class SQLiteHeader:
    def __init__(self, header_bytes: bytes) -> None:
        self._header_bytes: BytesOffsetArray = BytesOffsetArray(header_bytes)

    @property
    def page_size(self) -> int:
        raw_bytes = self._header_bytes.subbytes(
            offset=HeaderOffset.PAGE_SIZE.OFFSET,
            length=HeaderOffset.PAGE_SIZE.SIZE,
        )

        page_size = int.from_bytes(raw_bytes, byteorder="big", signed=False)
        # INFO: Value 1 represents a page size of 65536
        if page_size == 1:
            page_size = 65536

        self._validate_page_size(page_size)
        return page_size

    def _validate_page_size(self, page_size: int):
        if page_size < 512 or page_size > 32768:
            raise ValueError(
                f"Page size is {page_size}, but it needs in range [512, 32768]"
            )

        while page_size > 512:
            page_size, remainder = divmod(page_size, 2)
            if remainder != 0:
                raise ValueError("Page size needs to be a power of 2")

    @property
    def encoding(self) -> str:
        raw_bytes = self._header_bytes.subbytes(
            offset=HeaderOffset.DATABASE_TEXT_ENCODING.OFFSET,
            length=HeaderOffset.DATABASE_TEXT_ENCODING.SIZE,
        )

        encoding_value = int.from_bytes(raw_bytes, byteorder="big", signed=False)
        match encoding_value:
            case 1:
                return "utf-8"
            case 2:
                return "utf-16le"
            case 3:
                return "utf-16be"
            case _:
                raise ValueError("File corrupted, incorrect encoding value")


class SQLiteDatabase:
    def __init__(self, file_path: str | PathLike[str]) -> None:
        self._file: BinaryIO = cast(BinaryIO, open(file_path, "rb"))

        magic_file_header = self._file.read(HeaderOffset.HEADER_STRING.SIZE)
        if magic_file_header != HeaderOffset.SQLITE_MAGIC_STRING:
            self._file.close()
            raise ValueError(
                "File is probably not a SQLite database - incorrect header"
            )

    def __enter__(self):
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: TracebackType | None,
    ):
        self._file.close()

    def header(self) -> SQLiteHeader:
        _ = self._file.seek(0)
        header_bytes = self._file.read(100)

        return SQLiteHeader(header_bytes)

    @property
    def _pages_count(self) -> int:
        file_size = fstat(self._file.fileno()).st_size
        return file_size // self.header().page_size

    def _read_page_data(self, page_number: int) -> bytes:
        if page_number < 1:
            raise ValueError("Pages are numbered from 1")
        if page_number > (count := self._pages_count):
            raise ValueError(f"Max page number is {count}, but {page_number} requested")

        page_size = self.header().page_size
        absolute_page_start = page_size * (page_number - 1)

        _ = self._file.seek(absolute_page_start)
        page_bytes = self._file.read(page_size)

        return page_bytes

    def _btree_page(self, page_number: int) -> BTreePage:
        page_data = self._read_page_data(page_number)
        return BTreePage(page_data=page_data, page_number=page_number)

    def _overflow_page(self, page_number: int) -> OverflowPage:
        page_data = self._read_page_data(page_number)
        return OverflowPage(page_data=page_data)

    def _table_cells_tree(
        self,
        starting_page_number: int,
    ) -> Iterator[TableBTreeLeafCell]:
        page = self._btree_page(page_number=starting_page_number)

        match page.header.page_type:
            case PageType.INTERIOR_TABLE:
                interior_cells = cast(Iterator[TableBTreeInteriorCell], page.cells())
                for cell in interior_cells:
                    yield from self._table_cells_tree(cell.left_pointer)

                right_page_number = cast(int, page.header.right_most_pointer)
                yield from self._table_cells_tree(right_page_number)
            case PageType.LEAF_TABLE:
                leaf_cells = cast(Iterator[TableBTreeLeafCell], page.cells())
                yield from leaf_cells
            case _:
                raise ValueError

    def _row_ids_from_index(
        self,
        page_number: int,
        lookup_value: bytes,
    ) -> Iterator[int]:
        page = self._btree_page(page_number=page_number)

        match page.header.page_type:
            case PageType.INTERIOR_INDEX:
                interior_cells = cast(Iterator[IndexBTreeInteriorCell], page.cells())

                for cell in interior_cells:
                    payload = self._load_full_payload(cell)
                    row_records = parse_records(payload)

                    if lookup_value == row_records[0].data:
                        yield from self._row_ids_from_index(
                            cell.left_pointer,
                            lookup_value,
                        )
                        yield int.from_bytes(
                            row_records[1].data,
                            byteorder="big",
                            signed=False,
                        )
                    elif lookup_value < row_records[0].data:
                        yield from self._row_ids_from_index(
                            cell.left_pointer,
                            lookup_value,
                        )
                        break
                else:
                    if right_pointer := page.header.right_most_pointer:
                        yield from self._row_ids_from_index(right_pointer, lookup_value)

            case PageType.LEAF_INDEX:
                leaf_cells = cast(Iterator[IndexBTreeLeafCell], page.cells())
                for cell in leaf_cells:
                    payload = self._load_full_payload(cell)
                    row_records = parse_records(payload)

                    if row_records[0].data == lookup_value:
                        yield int.from_bytes(
                            row_records[1].data,
                            byteorder="big",
                            signed=False,
                        )

            case _:
                raise ValueError

    def _records_by_row_id(
        self, starting_page_number: int, row_id: int
    ) -> TableBTreeLeafCell | None:
        page = self._btree_page(page_number=starting_page_number)

        match page.header.page_type:
            case PageType.INTERIOR_TABLE:
                interior_cells = cast(Iterator[TableBTreeInteriorCell], page.cells())
                for cell in interior_cells:
                    if row_id <= cell.integer_key:
                        return self._records_by_row_id(cell.left_pointer, row_id)
                else:
                    right_page_number = cast(int, page.header.right_most_pointer)
                    return self._records_by_row_id(right_page_number, row_id)
            case PageType.LEAF_TABLE:
                leaf_cells = cast(Iterator[TableBTreeLeafCell], page.cells())
                for cell in leaf_cells:
                    if cell.row_id == row_id:
                        return cell

                return None
            case _:
                raise ValueError

    def _load_full_payload(
        self,
        leaf_cell: TableBTreeLeafCell | IndexBTreeInteriorCell | IndexBTreeLeafCell,
    ):
        remaining_bytes = leaf_cell.payload_size - len(leaf_cell.initial_payload)

        full_payload = leaf_cell.initial_payload
        next_overflow_page = leaf_cell.overflow_page

        while remaining_bytes > 0 and next_overflow_page is not None:
            overflow_page = self._overflow_page(next_overflow_page)
            data_chunk = overflow_page.overflow_data[:remaining_bytes]

            full_payload += data_chunk
            remaining_bytes -= len(data_chunk)
            next_overflow_page = overflow_page.next_overflow_page

        if leaf_cell.payload_size != len(full_payload):
            raise ValueError(
                f"Expected {leaf_cell.payload_size}, but got {len(full_payload)}"
            )

        return full_payload

    def schema_objects(self) -> Iterator[SchemaObject]:
        for leaf_cell in self._table_cells_tree(starting_page_number=1):
            full_payload = self._load_full_payload(leaf_cell)
            schema_table = SchemaObject.from_payload(
                BytesOffsetArray(full_payload),
                self.header().encoding,
            )
            yield schema_table

    def _extract_columns(self, table_sql: str, selected_columns: list[str]):
        keyword_replace = re.compile(re.escape("domain"), re.IGNORECASE)
        table_sql = keyword_replace.sub('"domain"', table_sql)

        sql_tokens: list[Token] = [
            token
            for token in cast(list[Token], sqlparse.parse(table_sql)[0].tokens)
            if not token.is_whitespace and not token.is_newline
        ]

        schema_column_names: list[str] = []
        if isinstance(parenthesis_token := sql_tokens[-1], Parenthesis):
            for token in cast(Iterator[Token], parenthesis_token.get_sublists()):
                if isinstance(token, IdentifierList):
                    identifiers = cast(Iterator[Token], token.get_identifiers())
                    token = list(identifiers)[-1]

                cleaned_value = token.value
                if cleaned_value[0] == '"' and cleaned_value[-1] == '"':
                    cleaned_value = cleaned_value[1:-1]

                schema_column_names.append(cleaned_value)
        else:
            raise ValueError("Unable to parse columns")

        selected_column_indices: list[int] = []
        for selected_column in selected_columns:
            if selected_column not in schema_column_names:
                raise ValueError(f"Column {selected_column} does not exist")
            selected_column_indices.append(schema_column_names.index(selected_column))

        return schema_column_names, selected_column_indices

    def _extract_indices(self, index_objects: list[SchemaObject]):
        column_root_page_map: dict[str, int] = {}

        for index_object in index_objects:
            if not index_object.root_page:
                raise ValueError

            sql_tokens: list[Token] = [
                token
                for token in cast(
                    list[Token], sqlparse.parse(index_object.sql)[0].tokens
                )
                if not token.is_whitespace and not token.is_newline
            ]

            if isinstance(function_token := sql_tokens[-1], Function) and isinstance(
                parenthesis_token := list(function_token.get_sublists())[-1],
                Parenthesis,
            ):
                for token in cast(Iterator[Token], parenthesis_token.get_sublists()):
                    if isinstance(token, IdentifierList):
                        identifiers = cast(Iterator[Token], token.get_identifiers())
                        token = list(identifiers)[-1]

                    column_root_page_map[token.value] = index_object.root_page
            else:
                raise ValueError("Unable to parse index")

        return column_root_page_map

    def _record_extractor(self, db_encoding: str, schema_column_names: list[str]):
        def extract(token: Token, row_record: list[Record]):
            record_value: Record
            if isinstance(token, Identifier):
                column_index = schema_column_names.index(token.value)
                record_value = row_record[column_index]
            else:
                if token.ttype is Literal.String.Single:
                    string_value = token.value[1:-1]
                    record_value = Record(
                        type=SerialType.STRING,
                        data=string_value.encode(db_encoding),
                    )
                elif token.ttype is Literal.Number.Integer:
                    record_value = Record(
                        type=SerialType.INT64,
                        data=int(token.value).to_bytes(),
                    )
                else:
                    raise ValueError(f"Unsupported value {token.value}")

            return record_value

        return extract

    def _extract_schema_table_objects(self, table_name: str):
        related_schema_objects = (
            schema_table
            for schema_table in self.schema_objects()
            if schema_table.tbl_name == table_name
        )

        table_schema = next(
            schema_object
            for schema_object in related_schema_objects
            if schema_object.is_table
        )
        table_index_schema = [
            schema_object
            for schema_object in related_schema_objects
            if schema_object.is_index
        ]

        return table_schema, table_index_schema

    def query(
        self,
        table_name: str,
        selected_columns: list[str],
        conditions: list[tuple[Token, Token]],
        count_rows: bool = False,
    ):
        table_schema, table_index_schema = self._extract_schema_table_objects(
            table_name
        )
        if not table_schema.root_page:
            raise ValueError(f"Table {table_name} not found in the database")

        linear_row_leaf_cells = self._table_cells_tree(
            starting_page_number=table_schema.root_page
        )
        if count_rows:
            yield len(list(linear_row_leaf_cells))
            return

        db_encoding = self.header().encoding
        schema_column_names, selected_column_indices = self._extract_columns(
            table_sql=table_schema.sql,
            selected_columns=selected_columns,
        )
        index_root_page_map = self._extract_indices(table_index_schema)

        indexable_conditions: list[tuple[Identifier, Token]] = []
        for left_arg, right_arg in conditions:
            is_left_identifier = isinstance(left_arg, Identifier)
            is_right_identifier = isinstance(right_arg, Identifier)

            if (is_left_identifier and is_right_identifier) or (
                not is_left_identifier and not is_right_identifier
            ):
                continue

            if is_left_identifier:
                indexable_conditions.append((left_arg, right_arg))
            elif is_right_identifier:
                indexable_conditions.append((right_arg, left_arg))

        index_condition_groups: list[list[TableBTreeLeafCell]] = []
        for condition_identifier, condition_value in indexable_conditions:
            if condition_identifier.value not in index_root_page_map:
                continue

            index_root_page = index_root_page_map[condition_identifier.value]

            value: bytes
            if condition_value.ttype is Literal.String.Single:
                value = condition_value.value[1:-1].encode(db_encoding)
            else:
                value = int(condition_value.value).to_bytes(
                    byteorder="big",
                    signed=True,
                )

            row_ids = self._row_ids_from_index(
                page_number=index_root_page,
                lookup_value=value,
            )

            filtered_cells: list[TableBTreeLeafCell] = []
            for id in row_ids:
                if cell := self._records_by_row_id(table_schema.root_page, id):
                    filtered_cells.append(cell)

            index_condition_groups.append(filtered_cells)

        if len(index_condition_groups) > 0:
            filtering_result: list[TableBTreeLeafCell] = index_condition_groups[0]
            for group in index_condition_groups[1:]:
                for cell in filtering_result:
                    if cell not in group:
                        filtering_result.remove(cell)

            linear_row_leaf_cells = iter(filtering_result)

        record_from_token = self._record_extractor(db_encoding, schema_column_names)
        for leaf_cell in linear_row_leaf_cells:
            payload = self._load_full_payload(leaf_cell)
            row_records = parse_records(payload)

            matching_row = True
            for left_token_arg, right_token_arg in conditions:
                left_record_value = record_from_token(left_token_arg, row_records)
                right_record_value = record_from_token(right_token_arg, row_records)

                if left_record_value != right_record_value:
                    matching_row = False
                    break

            if not matching_row:
                continue

            result: list[str] = []
            for index in selected_column_indices:
                if index == 0 and row_records[index].type == SerialType.NULL:
                    result.append(str(leaf_cell.row_id))
                else:
                    result.append(row_records[index].data.decode(db_encoding))

            yield result
