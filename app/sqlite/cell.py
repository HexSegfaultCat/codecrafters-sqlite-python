from dataclasses import dataclass

from .utils import BytesOffsetArray, huffman_varint


def _extract_data_with_overflow(
    cell_bytes: BytesOffsetArray,
    cell_data_start: int,
    full_payload_size: int,
) -> tuple[bytes, int | None]:
    cell_data_size = len(cell_bytes) - cell_data_start

    if cell_data_size >= full_payload_size:
        initial_data = cell_bytes.subbytes(
            offset=cell_data_start,
            length=full_payload_size,
        )

        return (initial_data, None)
    else:
        initial_data = cell_bytes[cell_data_start:-4]
        overflow_page = int.from_bytes(
            cell_bytes[-4:],
            byteorder="big",
            signed=False,
        )

        return (initial_data, overflow_page)


@dataclass
class TableBTreeLeafCell:
    payload_size: int
    row_id: int
    initial_payload: BytesOffsetArray
    overflow_page: int | None

    @staticmethod
    def create(cell_bytes: BytesOffsetArray):
        raw_size_bytes = cell_bytes.subbytes(offset=0, length=9)
        total_size_varint = huffman_varint(raw_size_bytes)

        raw_rowid_bytes = cell_bytes.subbytes(
            offset=(total_size_varint.length),
            length=9,
        )
        rowid_varint = huffman_varint(raw_rowid_bytes)

        initial_data, overflow_page = _extract_data_with_overflow(
            cell_bytes,
            cell_data_start=total_size_varint.length + rowid_varint.length,
            full_payload_size=total_size_varint.value,
        )

        return TableBTreeLeafCell(
            payload_size=total_size_varint.value,
            row_id=rowid_varint.value,
            initial_payload=BytesOffsetArray(initial_data),
            overflow_page=overflow_page,
        )


@dataclass
class IndexBTreeLeafCell:
    payload_size: int
    initial_payload: BytesOffsetArray
    overflow_page: int | None

    @staticmethod
    def create(cell_bytes: BytesOffsetArray):
        raw_size_bytes = cell_bytes.subbytes(offset=0, length=9)
        total_size_varint = huffman_varint(raw_size_bytes)

        initial_data, overflow_page = _extract_data_with_overflow(
            cell_bytes,
            cell_data_start=total_size_varint.length,
            full_payload_size=total_size_varint.value,
        )

        return IndexBTreeLeafCell(
            payload_size=total_size_varint.value,
            initial_payload=BytesOffsetArray(initial_data),
            overflow_page=overflow_page,
        )


@dataclass
class TableBTreeInteriorCell:
    left_pointer: int
    integer_key: int

    @staticmethod
    def create(cell_bytes: BytesOffsetArray):
        raw_left_pointer_bytes = cell_bytes[:4]
        left_pointer = int.from_bytes(
            raw_left_pointer_bytes,
            byteorder="big",
            signed=False,
        )

        raw_key_bytes = cell_bytes.subbytes(offset=4, length=9)
        key_varint = huffman_varint(raw_key_bytes)

        return TableBTreeInteriorCell(
            left_pointer=left_pointer,
            integer_key=key_varint.value,
        )


@dataclass
class IndexBTreeInteriorCell:
    left_pointer: int
    payload_size: int
    initial_payload: BytesOffsetArray
    overflow_page: int | None

    @staticmethod
    def create(cell_bytes: BytesOffsetArray):
        raw_left_pointer_bytes = cell_bytes[:4]
        left_pointer = int.from_bytes(
            raw_left_pointer_bytes,
            byteorder="big",
            signed=False,
        )

        raw_size_bytes = cell_bytes.subbytes(offset=4, length=9)
        total_size_varint = huffman_varint(raw_size_bytes)

        initial_data, overflow_page = _extract_data_with_overflow(
            cell_bytes,
            cell_data_start=total_size_varint.length + 4,
            full_payload_size=total_size_varint.value,
        )

        return IndexBTreeInteriorCell(
            left_pointer=left_pointer,
            payload_size=total_size_varint.value,
            initial_payload=BytesOffsetArray(initial_data),
            overflow_page=overflow_page,
        )


AnyBTreeCell = (
    TableBTreeLeafCell
    | IndexBTreeLeafCell
    | TableBTreeInteriorCell
    | IndexBTreeInteriorCell
)
