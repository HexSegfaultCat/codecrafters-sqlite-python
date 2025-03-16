import unittest

from .utils import huffman_varint


class TestUtils(unittest.TestCase):
    def test_one_byte_varint(self):
        expectedValue = 0b_0101_0110

        result = huffman_varint(bytes([expectedValue]))
        self.assertEqual(1, result.length)
        self.assertEqual(expectedValue, result.value)

    def test_multi_byte_varint(self):
        byte1 = 0b_1_101_0110
        byte2 = 0b_1_100_0100
        byte3 = 0b_0_010_0100
        _mask = 0b_0_111_1111

        expectedValue = (byte1 & _mask) << 7
        expectedValue = (expectedValue | (byte2 & _mask)) << 7
        expectedValue = expectedValue | (byte3 & _mask)

        result = huffman_varint(bytes([byte1, byte2, byte3]))
        self.assertEqual(3, result.length)
        self.assertEqual(expectedValue, result.value)


if __name__ == "__main__":
    _ = unittest.main()
