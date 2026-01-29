from lstore.page import Page

class PageDirectory:
    def __init__(self):
        pass

    def findRow(self, RID):
        # given a certain RID, return the correlating indirection column, schema column, 
        # and data columns, with the slot # (basically, which row)
        pass

    def findWritablePage(self, column):
        # given a column from a table, return the current writable page
        pass

    def newPage(self, column):
        # when a column runs out of space, create a new page for that column
        pass
