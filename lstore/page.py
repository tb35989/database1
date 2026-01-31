class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def has_capacity(self):
        maxCapacity = 4096 // 8
        if maxCapacity > self.num_records: # Checks capacity
            return True
        else:
            return False

    # note: when writing data in, call the write function from the page_list.py file,
    # not this one
    def write(self, value):
        offset = self.num_records * 8
        self.data[offset:offset+8] = value.to_bytes(8, byteorder='big')
        self.num_records += 1
        pass

    def find(self, value):
        # return the slot #/offset # for the given data (needs to be coded)
        # if the data is not found, return "not found"
        pass