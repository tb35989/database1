from lstore.page import Page

# Does everything the page directory needs to do, except linking RIDs to pages.

class PageList:
    # create the pagelist
    def __init__(self):
        self.connectedColumns = []
        newPage = Page()
        self.connectedColumns.append(newPage)

    # return the current writeable column
    def writableColumn(self):
        return self.connectedColumns[-1]

    # write data into the correct writeable column. if the column is full,
    # add a new page to the pagelist
    def write(self, value):
        if self.writeableColumn.has_capacity() > 0:
            self.writeableColumn.write(value)
        else:
            newPage = Page()
            self.connectedColumns.append(newPage)
            newPage.write(value)

    
