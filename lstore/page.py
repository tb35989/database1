
class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass


# Notes:
# Table is column oriented so each page is a column
# each column is a list of the data
# have page ranges so like 0-99, 100-199 (makes things faster/easier)
# ^requirement for this database
# Page directory: need to map each RID into the physical locations of the data
# The locations are in the form of a list (page, offset)
# offset is kind of like the row number, for every individual page
# 1 page has multiple rows

# if the page runs out of space, create a new page and put the whole entry into a new page