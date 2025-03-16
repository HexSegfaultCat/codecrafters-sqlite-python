from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, cast, final

from os import fstat, PathLike

from .table import Table
from .cell import TableBTreeInteriorCell, TableBTreeLeafCell
from .page import BTreePage, PageType
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
    # TODO: Add the missing ones


class SQLiteDatabase:
    def __init__(self, file_path: str | PathLike[str]) -> None:
        self._file: BinaryIO = cast(BinaryIO, open(file_path, "rb"))

        header = self._file.read(HeaderOffset.HEADER_STRING.SIZE)
        if header != HeaderOffset.SQLITE_MAGIC_STRING:
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

    @property
    def _pages_count(self) -> int:
        file_size = fstat(self._file.fileno()).st_size
        return file_size // self.page_size

    @property
    def page_size(self) -> int:
        _ = self._file.seek(HeaderOffset.PAGE_SIZE.OFFSET)
        raw_bytes = self._file.read(HeaderOffset.PAGE_SIZE.SIZE)

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

    def _page(self, page_number: int) -> BTreePage:
        if page_number < 1:
            raise ValueError("Pages are numbered from 1")
        if page_number > (count := self._pages_count):
            raise ValueError(f"Max page number is {count}")

        page_size = self.page_size
        absolute_page_start = page_size * (page_number - 1)

        _ = self._file.seek(absolute_page_start)
        page_bytes = BytesOffsetArray(self._file.read(page_size))

        return BTreePage(page_data=page_bytes, page_number=page_number)

    def _table_cells_tree(
        self,
        starting_page_number: int,
    ) -> Iterator[TableBTreeLeafCell]:
        page = self._page(page_number=starting_page_number)

        match page.header.page_type:
            case PageType.INTERIOR_TABLE:
                right_page_number = cast(int, page.header.right_most_pointer)
                yield from self._table_cells_tree(right_page_number)

                interior_cells = cast(Iterator[TableBTreeInteriorCell], page.cells())
                for cell in interior_cells:
                    yield from self._table_cells_tree(cell.left_pointer)
            case PageType.LEAF_TABLE:
                leaf_cells = cast(Iterator[TableBTreeLeafCell], page.cells())
                for cell in leaf_cells:
                    yield cell
            case _:
                raise ValueError

    def tables(self) -> Iterator[Table]:
        for cell in self._table_cells_tree(starting_page_number=1):
            _ = cell
            yield Table()
