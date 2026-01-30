from lstore.page import Page

# Does everything the page directory needs to do, except linking RIDs to pages.

class PageList:
    # create the pagelist: a list of pages that together create a column. think of it 
    # like a vertical list of columns.
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

    # returns a list where the first item is the page where the data is found
    # and the second item is the slot #/offset # where the data is found on that page
    # if the data is not found, returns "not found"
    def find(self, value):
        j = 0
        location = []
        while j < len(self.connectedColumns):
            if self.connectedColumns[j].find(value) == "not found":
                j += 1
            else:
                location.append(self.connectedColumns[j])
                location.append(self.connectedColumns[j].find(value))
                return location
        return "not found"

    
