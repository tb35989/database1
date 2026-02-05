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
            newColumn = PageList(baseOrTail = True)
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
    
    # writes data into a new base record row. assumes the RID 
    # column is the first column in the table, and then writes the rest of the data (record)
    # into the next columns
    # also checks to make sure the row all has the same slot #
    def write_base_record(self, rid, record, rid_col):
        slotNumbers = []
        slotNumbers.append(self.pageDirectoryBase[rid_col].write(rid))
        for i in range(len(record)):
            if i != rid_col:
                slotNumbers.append(self.pageDirectory[i].write(record[i]))
        for i in range(len(slotNumbers) - 1):
            if slotNumbers[0] != slotNumbers[i + 1]:
                return "error"
        return "success"

    # given an RID value, return the corresponding pages that contain the data,
    # and the slot number that all the data is on (assumes all the data is at the same slot #)
    # (assumes RID is the first column in the table) (includes RID's page as the first item)
    # the first value in the list returned is the slot number, then comes the pages in order
    # (this can be changed around if need be)
    def getBaseRow(self, rid):
        location = self.pageDirectoryBase[0].find(rid)
        row = []
        if location == "not found":
            return "RID not found in this table"
        else:
            row.append(location[2]) # the slot # for all the data
            j = location[1] # j = the index of the page the data is on in the pagelist
            # so for example, if the data is on the second page of the RID column,
            # j = 1 (since the first page would be 0)
            # anyways,
            for i in range(len(self.pageDirectoryBase)):
                row.append(self.pageDirectoryBase[i].getPage(j))

        


    # TAIL RECORDS

    # adds the specified number of columns to the table. Doesn't write anything in though.
    def addColumnTail(self, numColumns):
        for i in range(numColumns):
            newColumn = PageList(baseOrTail = False)
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
        
    
    # given an RID value, return the corresponding pages that contain the data,
    # and the slot number that all the data is on (assumes all the data is at the same slot #)
    # (assumes RID is the first column in the table) (includes RID's page as the first item)
    # the first value in the list returned is the slot number, then comes the pages in order
    # (this can be changed around if need be)
    def getTailRow(self, rid):
        location = self.pageDirectoryTail[0].find(rid)
        row = []
        if location == "not found":
            return "RID not found in this table"
        else:
            row.append(location[2]) # the slot # for all the data
            j = location[1] # j = the index of the page the data is on in the pagelist
            # so for example, if the data is on the second page of the RID column,
            # j = 1 (since the first page would be 0)
            # anyways,
            for i in range(len(self.pageDirectoryTail)):
                row.append(self.pageDirectoryTail[i].getPage(j))
