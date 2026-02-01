from lstore.page_list import PageList

class PageDirectory:
    # pagedirectory: a list of pagelists that make up a table. think of it like a horizontal
    # list of pages
    def __init__(self):
        self.pageDirectory = []

    # adds the specified number of columns to the table. Doesn't write anything in though.
    def addColumn(self, numColumns):
        for i in range(numColumns):
            newColumn = PageList()
            self.pageDirectory.append(newColumn)

    # writes specified data into the next available slot in the specified column
    def writeValueIntoColumn(self, columnID, value):
        if columnID < len(self.pageDirectory):
            self.pageDirectory[columnID].write(value)
        else:
            return "column ID invalid"

    # given an RID, returns the corresponding columns (not including the RID column), 
    # unless the RID is not found.
    # (assumes the first column in the table is RID)
    def findPageLists(self, RID):
        if self.pageDirectory[0].find(RID) == "not found":
            return "RID not found"
        else: 
            return self.pageDirectory[1:]
        
    # given a columnID, returns the corresponding pageList
    def findColumn(self, columnID):
        if columnID < len(self.pageDirectory):
            return self.pageDirectory[columnID]
        else:
            return "column ID invalid"