from collections.abc import Iterable
from enum import Enum
from typing import Final, final

from .cell import (
    AnyBTreeCell,
    IndexBTreeInteriorCell,
    IndexBTreeLeafCell,
    TableBTreeInteriorCell,
    TableBTreeLeafCell,
)
from .utils import BytesOffsetArray, OffsetMetadata


@final
class HeaderOffset:
    # INFO: https://www.sqlite.org/fileformat.html#b_tree_pages
    PAGE_TYPE = OffsetMetadata(OFFSET=0, SIZE=1)
    FIRST_FREEBLOCK = OffsetMetadata(OFFSET=1, SIZE=2)
    CELLS_COUNT = OffsetMetadata(OFFSET=3, SIZE=2)
    CELL_CONTENT_START = OffsetMetadata(OFFSET=5, SIZE=2)
    FRAGMENTED_BYTES = OffsetMetadata(OFFSET=7, SIZE=1)
    RIGHT_MOST_POINTER = OffsetMetadata(OFFSET=8, SIZE=4)


class PageType(Enum):
    INTERIOR_INDEX = 2
    INTERIOR_TABLE = 5
    LEAF_INDEX = 10
    LEAF_TABLE = 13


class BTreeHeader:
    def __init__(self, data: bytes):
        self._data: BytesOffsetArray = BytesOffsetArray(data)

    @property
    def page_type(self) -> PageType:
        bytes = self._data.subbytes(
            offset=HeaderOffset.PAGE_TYPE.OFFSET,
            length=HeaderOffset.PAGE_TYPE.SIZE,
        )
        return PageType(bytes[0])

    @property
    def first_freeblock_start(self) -> int:
        bytes = self._data.subbytes(
            offset=HeaderOffset.FIRST_FREEBLOCK.OFFSET,
            length=HeaderOffset.FIRST_FREEBLOCK.SIZE,
        )
        return int.from_bytes(bytes, byteorder="big", signed=False)

    @property
    def cells_count(self) -> int:
        bytes = self._data.subbytes(
            offset=HeaderOffset.CELLS_COUNT.OFFSET,
            length=HeaderOffset.CELLS_COUNT.SIZE,
        )
        return int.from_bytes(bytes, byteorder="big", signed=False)

    @property
    def cell_content_start(self) -> int:
        bytes = self._data.subbytes(
            offset=HeaderOffset.CELL_CONTENT_START.OFFSET,
            length=HeaderOffset.CELL_CONTENT_START.SIZE,
        )

        cell_content_start = int.from_bytes(bytes, byteorder="big", signed=False)
        if cell_content_start == 0:
            cell_content_start = 65536

        return cell_content_start

    @property
    def cell_content_fragmented_free_bytes(self) -> int:
        bytes = self._data.subbytes(
            offset=HeaderOffset.CELL_CONTENT_START.OFFSET,
            length=HeaderOffset.CELL_CONTENT_START.SIZE,
        )
        return int(bytes[0])

    @property
    def right_most_pointer(self) -> int | None:
        if not self.is_12_byte_header:
            return None

        raw_bytes = self._data.subbytes(
            offset=HeaderOffset.RIGHT_MOST_POINTER.OFFSET,
            length=HeaderOffset.RIGHT_MOST_POINTER.SIZE,
        )
        return int.from_bytes(raw_bytes, byteorder="big", signed=False)

    @property
    def is_12_byte_header(self) -> bool:
        return self.page_type in [
            PageType.INTERIOR_INDEX,
            PageType.INTERIOR_TABLE,
        ]


class BTreePage:
    def __init__(self, page_data: bytes, page_number: int) -> None:
        self._page_data: BytesOffsetArray = BytesOffsetArray(page_data)
        self._header_offset: int = 100 if page_number == 1 else 0

        self.page_number: Final[int] = page_number
        self.header: BTreeHeader = BTreeHeader(
            self._page_data.subbytes(offset=self._header_offset, length=12)
        )

    def _cell_pointers(self) -> Iterable[int]:
        cell_pointer_offset = self._header_offset + (
            12 if self.header.is_12_byte_header else 8
        )

        for _ in range(self.header.cells_count):
            raw_bytes = self._page_data.subbytes(
                offset=cell_pointer_offset,
                length=2,
            )
            yield int.from_bytes(raw_bytes, byteorder="big", signed=False)

            cell_pointer_offset += 2

    def cells(self) -> Iterable[AnyBTreeCell]:
        asc_sorted_cell_pointers = sorted(self._cell_pointers())

        for cell_start, cell_end in zip(
            asc_sorted_cell_pointers,
            [*asc_sorted_cell_pointers[1:], len(self._page_data)],
        ):
            raw_bytes = BytesOffsetArray(self._page_data[cell_start:cell_end])

            match self.header.page_type:
                case PageType.LEAF_TABLE:
                    yield TableBTreeLeafCell.create(raw_bytes)
                case PageType.LEAF_INDEX:
                    yield IndexBTreeLeafCell.create(raw_bytes)
                case PageType.INTERIOR_TABLE:
                    yield TableBTreeInteriorCell.create(raw_bytes)
                case PageType.INTERIOR_INDEX:
                    yield IndexBTreeInteriorCell.create(raw_bytes)


class OverflowPage:
    def __init__(self, page_data: bytes) -> None:
        next_page_number = int.from_bytes(
            page_data[:4],
            byteorder="big",
            signed=False,
        )

        self.next_overflow_page: Final[int | None] = (
            next_page_number if next_page_number != 0 else None
        )
        self.overflow_data: Final[bytes] = page_data[4:]
