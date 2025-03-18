from dataclasses import dataclass
from enum import Enum

from .utils import BytesOffsetArray, huffman_varint


class SerialType(Enum):
    NULL = 0
    INT8 = 1
    INT16 = 2
    INT24 = 3
    INT32 = 4
    INT48 = 5
    INT64 = 6
    FLOAT64 = 7
    INT_ZERO = 8
    INT_ONE = 9
    RESERVED1 = 10
    RESERVED2 = 11
    BLOB = 12
    STRING = 13


@dataclass
class Record:
    type: SerialType
    data: bytes


def _parse_header(value: int) -> tuple[SerialType, int]:
    match value:
        case SerialType.NULL.value:
            return (SerialType(value), 0)
        case SerialType.INT8.value:
            return (SerialType(value), 1)
        case SerialType.INT16.value:
            return (SerialType(value), 2)
        case SerialType.INT24.value:
            return (SerialType(value), 3)
        case SerialType.INT32.value:
            return (SerialType(value), 4)
        case SerialType.INT48.value:
            return (SerialType(value), 6)
        case SerialType.INT64.value:
            return (SerialType(value), 8)
        case SerialType.FLOAT64.value:
            return (SerialType(value), 8)
        case SerialType.INT_ZERO.value:
            return (SerialType(value), 0)
        case SerialType.INT_ONE.value:
            return (SerialType(value), 0)
        case SerialType.RESERVED1.value:
            return (SerialType(value), 0)
        case SerialType.RESERVED2.value:
            return (SerialType(value), 0)
        case _ if value >= 12 and value % 2 == 0:
            return (SerialType.BLOB, (value - 12) // 2)
        case _ if value >= 13 and value % 2 == 1:
            return (SerialType.STRING, (value - 13) // 2)
        case _:
            raise ValueError


def parse_records(payload: bytes) -> list[Record]:
    header_size_varint = huffman_varint(payload[:9])
    header_bytes, body_bytes = (
        BytesOffsetArray(payload[: header_size_varint.value]),
        BytesOffsetArray(payload[header_size_varint.value :]),
    )

    header_offset = header_size_varint.length
    body_offset = 0

    records: list[Record] = []
    while header_offset < len(header_bytes):
        header_varint = huffman_varint(
            header_bytes.subbytes(offset=header_offset, length=9),
        )

        serial_type, size = _parse_header(header_varint.value)
        data = body_bytes.subbytes(offset=body_offset, length=size)

        records.append(Record(type=serial_type, data=data))

        body_offset += size
        header_offset += header_varint.length

    return records
