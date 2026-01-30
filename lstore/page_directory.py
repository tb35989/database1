from lstore.page_list import PageList

class PageDirectory:
    # pagedirectory: a list of all the tables in the dataset. the format is tableName, table
    # so the list might look something like this:
    # [apples, [applesColumn1, applesColumn2], bananas, [bananasColumn1, bananasColumn2, bananasColumn3]]
    def __init__(self):
        self.pageDirectory = []

    # adds a table to the page directory.
    def addTable(self, tableName):
        self.pageDirectory.append(tableName)
        actualTable = []
        self.pageDirectory.append(actualTable)

    # adds a column to the table of given name. the table class is responsible for 
    # remembering the index of each column. 
    def addColumnToTable(self, tableName):
        newColumn = PageList()
        # search the pagedirectory for the right table and 
        # append the column to the table
        if self.pageDirectory.index(tableName) >= 0:
            self.pageDirectory[self.pageDirectory.index(tableName) + 1].append(newColumn)
        else:
            return "table not found"
    
    # writes given data into the next available slot of columnIndex'th column of the
    # table with the name tableName. 
    def writeValueIntoColumn(self, tableName, columnIndex, value):
        # checks whether the table exists in the page directory, and 
        # whether the given columnIndex is a valid index
        if self.pageDirectory.index(tableName) >= 0 and (columnIndex + 1) <= len(self.pageDirectory[self.pageDirectory.index(tableName) + 1]):
            # writes the data into the column
            self.pageDirectory[self.pageDirectory.index(tableName) + 1][columnIndex].write(value)
        else:
            return "table not found or column index invalid"

    # given a certain RID, return the correlating pages
    # (assumes that RID is the first column in the table)
    def findRow(self, RID):
        # seach the page directory for the RID
        for i in range(len(self.pageDirectory)):
            i += 1
            if self.pageDirectory[i][0].find(RID) == "not found":
                i += 1
            else:
                return self.pageDirectory[i][1:]
        return "RID not found"
