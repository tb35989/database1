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

    def schema_mask(self, updated_cols): #TODO 
        pass

    #CUMULATIVE UPDATE 
    def make_tail_record(self,prev_rid,tail_rid,updated_cols): #Assumes input for updated_cols is a tuple with placeholders of "None" for columns without updates
        record = [0] * self.total_cols # Initializing record with list of 0s corresponding with number of cols
        record[INDIRECTION_COLUMN] = prev_rid #base_rid if first update, previous tail page if not
        record[RID_COLUMN] = tail_rid # Newly allocated tail_rid
        record[TIMESTAMP_COLUMN] = int(time()) 
        record[SCHEMA_ENCODING_COLUMN] = #SCHEMA MASK
        for i in range(self.num_columns):
            physical_col = 4 + i # Adjust column index for metadata columns
            if updated_cols[i] is not None: # If corresponding column has an update
                record[physical_col] = updated_cols[i]
            else:
                if prev_rid == base_rid: # For first update
                    record[physical_col] = self.read_base_value(base_rid, physical_col)
                else: # For later updates (previously existing tail pages)
                    record[physical_col] = self.read_tail_value[tail_rid, physical_col]
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

'FOLLOWING IS DUPLICATE OF QUERY.PY UPDATE FOR EDITS'
    def update(self,key,*updated_cols):
        tail_rid = self.new_tail_rid() # Assign tail rid to new record
        record = self.make_tail_record(tail_rid, updated_cols)
        # Call on read_base_value or read_tail_value in order to check indirection column for if updates have already been made
        # If its the first update, 
        # Go into page directory / page list 
        pass
   
    #UPDATE SCHEMA ENCODING COL AS WELL
    def update(self, primary_key, *columns):
        rid_list = self.table.index.locate(self.table.key, primary_key)
        #returns False if no records exist with given key or if record has been deleted
        if len(rid_list) == 0: 
            return False
        rid = rid_list[0]
        if rid == -1:
            return False
        b_page, b_slot = self.table.base_pd.pageDirectoryBase[1].find(rid) # RETURNS PAGE OBJ, SLOT ?? 
        b_index = self.table.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)
        indirection = self.table.read_base_value(base_rid, INDIRECTION_COLUMN) # USE HELPER FUNCTION FOR INDIRECTION
        if indirection == 0:
            #create new tail RID
            t_rid = self.new_tail_rid()
            #NEED TO UPDATE THE COLS OF THE NEW TAIL RECORD (WITH METADATA AND USER DATA) - IN PROGRESS
            record = self.make_tail_record(old_t_rid, new_t_rid, columns)
            #NEED TO WRITE THE TAIL RECORD TO THE TAIL PAGE DATA STRUCTURE 
            #update base record's indirection column
            self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].write(b_slot, t_rid)
        else:
            old_t_rid = indirection
            new_t_rid = self.new_tail_rid()
            #creating new tail record where its indirection points to the previous version
            record = self.make_tail_record(old_t_rid, new_t_rid, columns) # have to update cols in table.py function
            #NEED TO WRITE THE TAIL RECORD TO THE TAIL PAGE DATA STRUCTURE 
            #updating the base record so its indirection points to the latest tail record
            self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].write(b_slot, new_t_rid)
'END'

    #Set up a function to access the actual value given a RID and column
    #To be used for indirection setting (will specify column as INDIRECTION_COLUMN) 
    def read_base_value(self,base_rid,column): # Return value stored within a physical slot in a page
        rid_page_index, slot = self.pageDirectoryBase[RID_COLUMN].find(base_rid) # Use "find" function within page_list.py to locate slot for rid, returns page id and slot on that page
        page_list = self.pageDirectoryBase[column] # Locate page list for corresponding column 
        page = page_list.pages[rid_page_index] # Use the specific index of the RID in correct page list to find value for that observation, at that column
        return page.read(slot) # Return value stored at that specified slot

'HELPER FOR IF FIND RETURNS PAGE INSTEAD OF PAGE INDEX'
    def read_base_value(self, base_rid, column):
        page, slot = self.pageDirectoryBase[column].find(base_rid)
        return page.read(slot)

'HELPER FOR IF FIND RETURNS PAGE INSTEAD OF PAGE INDEX'
    def read_tail_value(self, tail_rid, column):
        page, slot = self.pageDirectoryTail[column].find(tail_rid)
        return page.read(slot)
        
    #Replicate above logic for tail pages
    def read_tail_value(self,tail_rid,column):
        rid_page_index, slot = self.pageDirectoryTail[RID_COLUMN].find(tail_rid)
        page_list = self.pageDirectoryTail[column]
        page = page_list.pages[rid_page_index]
        return page.read(slot)

#WORK IN PROGRESS 
    def get_version_rid(self, base_rid, relative_version):
        b_page, b_slot = self.base_pd.pageDirectoryBase[1].find(base_rid)
        b_index = self.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)
    
        indirection = self.base_pd.pageDirectoryBase[0].connectedColumns[b_index].read(b_slot)
    
        # If no tail updates, returns base RID
        if indirection == 0:
            return base_rid

        # Loops through until it reaches the relative version
        curr = indirection
        for i in range(abs(relative_version)):
            t_page, t_slot = self.tail_pd.pageDirectoryTail[1].find(curr)
            previous_indirection = self.tail_pd.pageDirectoryTail[0].connectedColumns[0].read(t_slot)
            if previous_indirection == 0:
                return base_rid
            curr = previous_indirection

        return curr
#also need to implement if schema encoding col is flagged, go back 1 version
#if value is null at the specific version, keep going back until value exists 

