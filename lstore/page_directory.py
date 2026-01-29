from lstore.page import Page

class PageDirectory:
    def __init__(self):
        pass

    # given a certain RID, return the correlating indirection column, schema column, 
    # and data columns, with the slot # (basically, which row)
    def findRow(self, RID):
        pass