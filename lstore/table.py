from lstore.index import Index
from time import time
from page_directory import PageDirectory

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.key_to_rid = {} # Set up dictionary for key to rid mapping
        self.num_columns = num_columns
        self.index = Index(self) 
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        # Initialize page directories for new base and tail page lists 
        self.total_cols = num_columns + 4
        self.base_pd = PageDirectory(self.total_cols)
        self.tail_pd = PageDirectory(self.total_cols)
        # Initialize base and tail RIDs
        self.next_base_rid = 1
        self.next_tail_rid = 1
        pass

    def __merge(self):
        print("merge is happening")
        pass

    # Allocate new RID when new base record is added
    def new_base_rid(self):
        base_rid = self.next_base_rid
        self.next_base_rid += 1
        return base_rid
        
    # Allocate new RID when tail record is added (for updates)
    def new_tail_rid(self):
        tail_rid = self.next_tail_rid
        self.next_tail_rid += 1
        return tail_rid
'''
num_columns = number of user columns
total_cols = number of physical columns (including metadata cols)
user_columns = list/tuple of values to fill into user columns
'''
    def make_base_record(self,base_rid,user_columns):
        record = [0] * self.total_cols # Generate list of 0s with correct number of columns 
        # INDIRECTION_COLUMN and SCHEMA_ENCODING_COLUMN initialized to 0
        # these values indicate no updates have been made yet
        record[RID_COLUMN] = base_rid # Fill in base RID column
        record[TIMESTAMP_COLUMN] = int(time()) # Fill in timestamp column
        for i, val in enumerate(user_columns): # Set loop to fill user column values into record
            record[4 + i] = val # Initialize index to ignore metadata cols (index 0-3)
        return record

    def insert(self,*user_columns):
        # Check parameters of insert (correct number of  user columns are included)
        if len(user_columns) != self.num_columns:
            return False
        # Check for duplicate key already in table
        key_val = user_columns[self.key] # Identify value of new key
        if key_val in self.key_to_rid: 
            return False
        
        base_rid = self.new_base_rid() # Assign base rid to new record
        record = self.make_base_record(base_rid, user_columns) # Call on make_base_record to fill in columns
        
        # REQUIRES CHANGES TO PAGE_DIRECTORY.PY - need method to write_base_record
        self.base_pd.write_base_record(rid=base_rid, record=record, rid_col=RID_COLUMN)

        self.key_to_rid[key] = base_rid # Add key and assigned RID of new observation to key->RID map 
        return True # Indicates success of insert

    def select(self,key):
        pass

    def update(self,key):
        pass

    def delete(self,key):
        pass

    #Set up a function to access the actual value given a RID and column
    #To be used for indirection setting (will specify column as INDIRECTION_COLUMN) 
    def read_base_value(self,rid,column): # Return value stored within a physical slot in a page
        rid_page_index, slot = self.pageDirectoryBase[RID_COLUMN].find(rid) # Use "find" function within page_list.py to locate slot for rid, returns page id and slot on that page
        page_list = self.pageDirectoryBase[column] # Locate page list for corresponding column 
        page = page_list.pages[rid_page_index] # Use the specific index of the RID in correct page list to find value for that observation, at that column
        return page.read(slot) # Return value stored at that specified slot

 
