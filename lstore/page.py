class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def has_capacity(self):
        maxCapacity = 4096 // 8
        return maxCapacity - self.num_records

    def write(self, value):
        offset = self.num_records * 8
        self.data[offset:offset+8] = value.to_bytes(8)
        self.num_records += 1
<<<<<<< HEAD
<<<<<<< Updated upstream
        # insert data into the index that is the number of records - 1 
        # (since bytearrays start at index 0)
        self.insert(self.num_records - 1, value)
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

# if the page runs out of space, create a new page
=======
        #self.data[0:8] = value.to_bytes(8), increment
        pass


# Partition table into different ranges 
#create six active tables for 6 columns - have each include new data. then update page directory
#create an array to maintain all the pages
#first go to page directory, then
#page directory is a map from a string to an array 
#create a new py file for page directory
>>>>>>> Stashed changes
=======
        pass
>>>>>>> 6752390d8854e16a004c61923f9c923847674377
