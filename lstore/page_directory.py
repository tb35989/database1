from lstore.page_list import PageList

class PageDirectory:
    # pagedirectory: a list of pagelists that make up a table. think of it like a horizontal
    # list of pages
    # contains a list of Base pages and a list of Tail pages. Both can perform the same 
    # functions, they are just kept differentiated.
    #are these a matrix?
    def __init__(self):
        self.pageDirectoryBase = []
        self.pageDirectoryTail = []

    # given an RID, returns the corresponding columns (not including the RID column), 
    # unless the RID is not found.
    # (assumes the first column in the table is RID)
    # (searches both base and tail records)
    def findPageLists(self, RID):
        if self.pageDirectoryTail[0].find(RID) == "not found":
            if self.pageDirectoryBase[0].find(RID) == "not found":
                return "RID not found"
            else:
                return self.pageDirectoryBase[1:]
        else: 
            return self.pageDirectoryTail[1:]



    # BASE RECORDS

    # adds the specified number of columns to the table. Doesn't write anything in though.
    def addColumnBase(self, numColumns):
        for i in range(numColumns):
            newColumn = PageList()
            self.pageDirectoryBase.append(newColumn)

    # writes specified data into the next available slot in the specified column
    # returns the slot number
    def writeValueIntoColumnBase(self, columnID, value):
        if columnID < len(self.pageDirectoryBase):
            return self.pageDirectoryBase[columnID].write(value)
        else:
            return "column ID invalid"
        
    # given a columnID, returns the corresponding pageList
    def findColumnBase(self, columnID):
        if columnID < len(self.pageDirectoryBase):
            return self.pageDirectoryBase[columnID]
        else:
            return "column ID invalid"
    
    # (a work in progress) writes data into a new base record row. assumes the RID 
    # column is the first column in the table, and then writes the rest of the data (record)
    # into the next columns
    # also checks to make sure the row all has the same slot #
    def write_base_record(self, rid, record, rid_col):
        slotNumbers = []
        slotNumbers.append(self.pageDirectoryBase[0].write(rid))
        for i in range(len(record)):
            slotNumbers.append(self.pageDirectory[i + 1].write(record[i]))
        for i in range(len(slotNumbers) - 1):
            if slotNumbers[0] != slotNumbers[i + 1]:
                return "error"
        return "success"
        

        


    # TAIL RECORDS

    # adds the specified number of columns to the table. Doesn't write anything in though.
    def addColumnTail(self, numColumns):
        for i in range(numColumns):
            newColumn = PageList()
            self.pageDirectoryTail.append(newColumn)

    # writes specified data into the next available slot in the specified column
    # returns the slot number
    def writeValueIntoColumnTail(self, columnID, value):
        if columnID < len(self.pageDirectoryTail):
            return self.pageDirectoryTail[columnID].write(value)
        else:
            return "column ID invalid"

    # given a columnID, returns the corresponding pageList
    def findColumnTail(self, columnID):
        if columnID < len(self.pageDirectoryTail):
            return self.pageDirectoryTail[columnID]
        else:
            return "column ID invalid"