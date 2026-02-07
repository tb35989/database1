class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def setPageRange(self, pageRange):
        self.pageRange = pageRange

    def has_capacity(self):
        maxCapacity = 4096 // 8
        if maxCapacity > self.num_records: # Checks capacity
            return True
        else:
            return False

    # note: when writing data in, call the write function from the page_directory.py file,
    # not this one
    # returns the location (slot/offset #) of the written in data
    def write(self, value):
        offset = self.num_records * 8
        self.data[offset:offset+8] = value.to_bytes(8, byteorder='big', signed = True)
        self.num_records += 1
        return self.num_records - 1
    
    def writeAtSlot(self, value, slot):
        offset = slot * 8 # Create offset within page based on slot number
        self.data[offset:offset+8] = value.to_bytes(8, byteorder='big', signed = True)

    def find(self, value):
        for slot in range(self.num_records):
            start = slot * 8
            end = start + 8
        if int.from_bytes(self.data[start:end], 'big', signed=True) == value:
            return slot
        return "not found"

        # return the slot #/offset # for the given data (needs to be coded)
        # if the data is not found, return "not found"

    def next_offset(self):
        return self.num_records * 8
    

'''class BasePage:
    def __init__(self, num_cols):
        self.rid = Page()
        self.indirection = Page()
        self.time = Page()
        self.schema_encoding = Page()
        self.pages = [Page() for _ in range(num_cols)]
    
    def get_offset(self):
        return self.rid.next_offset()
    
    def append_record(self, rid, record, rid_col):
        self.indirection.write(record[0])
        self.rid.write(record[1])
        self.time.write(record[2])
        self.schema_encoding.write(record[3])
        for i in range(4, len(record)):
            self.pages[i-4].write(record[i])

class TailPage:
    def __init__(self, num_cols):
        self.rid = Page()
        self.indirection = Page()
        self.time = Page()
        self.schema_encoding = Page()
        self.pages = [Page() for _ in range(num_cols)]

    def get_offset(self):
        return self.rid.next_offset()
    
    def append_record(self, rid, record, rid_col):
        self.indirection.write(record[0])
        self.rid.write(record[1])
        self.time.write(record[2])
        self.schema_encoding.write(record[3])
        for i in range(4, len(record)):
            self.pages[i-4].write(record[i])'''