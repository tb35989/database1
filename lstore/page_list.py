from lstore.page import Page

# I'm a little confused as to how the page directory works. This file performs some of the 
# functions the page directory is supposed to do, but I don't know how to connect RIDs to 
# adjacent columns in this file. It seems to me that that would need to go in the table file?

class PageList:
    # create the pagelist
    def __init__(self):
        self.connectedColumns = []

    # add a new page to the pagelist
    def connectAColumn(self):
        newPage = Page()
        self.connectedColumns.append(newPage)
    
    # return the current writeable column
    def writableColumn(self):
        return self.connectedColumns[-1]

    
