from lstore.page import Page

# Does everything the page directory needs to do, except linking RIDs to pages.

class PageList:

    # given the primary key, should return the correct page range of base pages
    # ask about in office hours

    # create the pagelist: a list of pages that together create a column. think of it 
    # like a vertical list of columns.
    def __init__(self, baseOrTail):
        self.connectedColumns = []
        newPage = Page()
        self.connectedColumns.append(newPage)
        self.base = baseOrTail  # set to True if base page, False if not
        if self.base:
            self.counter = 0

    # return the current writeable column
    def writableColumn(self):
        return self.connectedColumns[-1]

    # write data into the correct writeable column. if the column is full,
    # add a new page to the pagelist
    # also, returns the location (slot #/offset #) of the new data
    def write(self, value):
        if self.writeableColumn.has_capacity() > 0:
            return self.writeableColumn.write(value)
        else:
            newPage = Page()
            self.counter += 1
            self.connectedColumns.append(newPage)
            return newPage.write(value)

    # given a page range, returns the pages in the PageList that correspond
    # to that page range. For example, 1 returns the first 16 pages, 2 
    # returns the 17th - the 32nd pages
    def getPageRange(self, pageRange):
        range = pageRange - 1
        if len(self.connectedColumns) < (range * 16 + 15):
            # ex: if pageRange = 1 and there are 3 pages in the pagelist, return 
            # ((1 - 1) * 16):the end of the list AKA return 0:2
            return self.connectedColumns[(range * 16):]
        else:
            # ex: if pagerange = 1 and there are 17 pages in the pagelist, return
            # ((1 - 1) * 16):((1 - 1) * 16 + 15)
            # = return self.connectedColumns[0:15] aka the first 16 pages
            return self.connectedColumns[(range * 16):(range * 16 + 15)]

    # returns a list where the first item is the page where the data is found,
    # the second item is the index of the page in the pagelist where the data is found,
    # and the third item is the slot #/offset # where the data is found on that page
    # if the data is not found, returns "not found"
    def find(self, value):
        j = 0
        location = []
        while j < len(self.connectedColumns):
            if self.connectedColumns[j].find(value) == "not found":
                j += 1
            else:
                location.append(self.connectedColumns[j])
                location.append(j)
                location.append(self.connectedColumns[j].find(value))
                return location
        return "not found"

    # given the page #, returns the corresponding page. starts at 0, so 0 returns 
    # the first page in the pagelist, 1 returns the second page, and so on. 
    # should correspond with j from the find() function above
    def getPage(self, index):
        return self.connectedColumns[index]
    