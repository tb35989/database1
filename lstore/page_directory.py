from lstore.page_list import PageList

class PageDirectory:
    # pagedirectory: a list of all the tables in the dataset.
    def __init__(self):
        self.pageDirectory = []

    # adds a table to the page directory.
    def addTable(self, tableName):
        newTable = []
        newTable.append(tableName)
        actualTable = []
        newTable.append(actualTable)
        self.pageDirectory.append(newTable)

    def addColumnToTable(self, tableName, column):
        newColumn = PageList()
        newColumn.write(column)
        # search the pagedirectory for the right table and 
        # append the column to the table
        for i in len(self.pageDirectory):
            # self.table[].append(newColumn)
            pass

    # given a certain RID, return the correlating pages
    # (assumes that RID is the first column in the table)
    # 
    def findRow(self, RID):
        pass