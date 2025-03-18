from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, cast, final

from os import fstat, PathLike

from .schema import SchemaTable
from .cell import TableBTreeInteriorCell, TableBTreeLeafCell
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
                right_page_number = cast(int, page.header.right_most_pointer)
                yield from self._table_cells_tree(right_page_number)

                interior_cells = cast(Iterator[TableBTreeInteriorCell], page.cells())
                for cell in interior_cells:
                    yield from self._table_cells_tree(cell.left_pointer)
            case PageType.LEAF_TABLE:
                leaf_cells = cast(Iterator[TableBTreeLeafCell], page.cells())
                yield from leaf_cells
            case _:
                raise ValueError

    def _load_full_payload(self, leaf_cell: TableBTreeLeafCell):
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

    def schema_tables(self) -> Iterator[SchemaTable]:
        for leaf_cell in self._table_cells_tree(starting_page_number=1):
            full_payload = self._load_full_payload(leaf_cell)
            schema_table = SchemaTable.from_payload(
                BytesOffsetArray(full_payload),
                self.header().encoding,
            )
            yield schema_table
