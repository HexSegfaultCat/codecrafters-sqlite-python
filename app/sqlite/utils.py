from typing import NamedTuple, final


@final
class OffsetMetadata(NamedTuple):
    OFFSET: int
    SIZE: int


class HuffmanResult(NamedTuple):
    value: int
    length: int


def huffman_varint(bytes: bytes) -> HuffmanResult:
    if len(bytes) < 1 or len(bytes) > 9:
        raise ValueError

    def is_last(byte: int):
        return not bool(byte >> 7)

    last_varint_byte_index = next(
        index for index, byte in enumerate(bytes) if is_last(byte)
    )

    value, length = 0, last_varint_byte_index + 1
    for byte in reversed(bytes[:length]):
        value <<= 7
        value |= 0b_0111_1111 & byte

    return HuffmanResult(value, length)


class BytesOffsetArray(bytes):
    def subbytes(self, offset: int, length: int) -> bytes:
        return self[offset : (offset + length)]
