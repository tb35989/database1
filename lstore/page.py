class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def has_capacity(self):
        maxCapacity = 4096 // 8
        return maxCapacity - self.num_records

    def write(self, value):
        offset = self.num_records * 8
        self.data[offset:offset+8] = value.to_bytes(8)
        self.num_records += 1
        pass