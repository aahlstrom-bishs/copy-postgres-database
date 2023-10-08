import numpy as np


class BitFlags:
    def __init__(self, num_flags: int):
        self.num_flags = num_flags
        if num_flags == 0:
            return
        num_ints = np.uint8(np.ceil(num_flags / 64))
        self.flags = np.zeros(num_ints, dtype=np.uint64)

    def from_flags(flags: np.ndarray):
        bit_flags = BitFlags(0)
        bit_flags.flags = flags
        bit_flags.num_flags = len(flags) * 64
        return bit_flags

    def set_bit(self, bit: int) -> np.ndarray:
        [index, offset] = self.get_address(bit)
        self.flags[index] = np.uint64(int(self.flags[index]) | (1 << int(offset)))

    def get_bit(self, bit):
        [index, offset] = self.get_address(bit)
        return bool(int(self.flags[index]) & (1 << int(offset)))

    def get_address(self, bit: int) -> np.ndarray:
        if self.flags.size == 0:
            raise ValueError("Array must not be empty")
        if np.dtype(self.flags[0]) != np.dtype(np.uint64):
            raise ValueError("Array must be of type uint64")
        index = bit // 64
        offset = bit % 64
        return np.array([index, offset], dtype=np.uint8)

    def get_flags(self):
        return [i for i in range(self.num_flags) if self.get_bit(i)]

    def bit_or(self, a, b):
        return np.uint64(int(a) | int(b))

    def bit_and(self, a, b):
        return np.uint64(int(a) & int(b))

    def bit_xor(self, a, b):
        return np.uint64(int(a) ^ int(b))

    def bit_not(self, a):
        return np.uint64(~int(a))

    def bit_shift_left(self, a, b):
        return np.uint64(int(a) << int(b))

    def bit_shift_right(self, a, b):
        return np.uint64(int(a) >> int(b))