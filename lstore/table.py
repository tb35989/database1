from lstore.index import Index
from time import time
from lstore.page_range import PageRange

# CHANGE RID TO COLUMN 0 
RID_COLUMN = 0
INDIRECTION_COLUMN = 1
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
        self.page_range = PageRange(self.total_cols)
        # Initialize base and tail RIDs
        self.next_base_rid = 1
        self.next_tail_rid = 1

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

    def schema_mask(self, updated_cols): #Input is list with index of each column that is updated
        mask = 0 # Initialize bitmask 
        for i in updated_cols: # Iterate for every column that was updated
            mask |= (1 << i) # Starting with ..0001, shift 1 left by number i (columns are ordered colk, ..., col1, col0)
            # (1 << i ) returns integer represented by binary mask
            # |= operator retains indicators for previously changed columns
        return mask

    #CUMULATIVE UPDATE 
    def make_tail_record(self,base_rid, prev_rid, tail_rid, updated_cols): #Assumes input for updated_cols is a tuple with placeholders of "None" for columns without updates
        record = [0] * self.total_cols # Initializing record with list of 0s corresponding with number of cols
        record[INDIRECTION_COLUMN] = prev_rid #base_rid if first update, previous tail page if not
        record[RID_COLUMN] = tail_rid # Newly allocated tail_rid
        record[TIMESTAMP_COLUMN] = int(time()) 
        # Extract indices of columns where updates were made 
        updated_indices = []
        for i, v in enumerate(updated_cols):
            if v is not None: # Account for columns with no updates
                updated_indices.append(i)
        record[SCHEMA_ENCODING_COLUMN] = self.schema_mask(updated_indices) # Pass list of indices into schema mask
        for i in range(self.num_columns):
            physical_col = 4 + i # Adjust column index for metadata columns
            if updated_cols[i] is not None: # If corresponding column has an update
                record[physical_col] = updated_cols[i]
            else:
                if prev_rid == base_rid: # For first update
                    record[physical_col] = self.read_base_value(base_rid, physical_col)
                else: # For later updates (previously existing tail pages)
                    record[physical_col] = self.read_tail_value(prev_rid, physical_col)
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
        
        self.page_range.write_base_record(rid=base_rid, record=record, rid_col=RID_COLUMN)

        self.key_to_rid[key_val] = base_rid # Add key and assigned RID of new observation to key->RID map 
        return True # Indicates success of insert


    def read_base_value(self, base_rid, column):
        row = self.page_range.getBaseRow(base_rid) # Returns list with slot number at index 0 and page objects for corresponding cols
        if isinstance(row, str): # Check if RID was actually found in table
            return None
        slot = row[0]
        page_for_col = row[1+column] # Returns page for specified column 
        return page_for_col.read(slot)

    def read_tail_value(self, tail_rid, column): 
        row = self.page_range.getTailRow(tail_rid)
        if isinstance(row, str):
            return None
        slot = row[0]
        page_for_col = row[1+column]
        return page_for_col.read(slot)

    def invalidate_tail_rid(self, tail_rid):
        row = self.page_range.getTailRow(tail_rid) # Get tail record
        if isinstance(row, str): # Check that rid exists
            return False
        slot = row[0] # Locate slot for given record
        rid_page = row[1 + RID_COLUMN] # Go to correct slot and page number
        rid_page.writeAtSlot(-1, slot) # Invalidate
        return True
    
    def rid_invalidation(self, base_rid):
        base_row = self.page_range.getBaseRow(base_rid) # Get row for record to invalidate
        if isinstance(base_row, str): # Check that rid exists (error message would return)
            return False
        
        slot = base_row[0] # Locate slot for given record
        latest = base_row[1 + INDIRECTION_COLUMN].read(slot) # Collect latest tail rid for chain traversal
        base_row[1 + RID_COLUMN].writeAtSlot(-1, slot) # Invalidate base RID
        base_row[1 + INDIRECTION_COLUMN].writeAtSlot(-1, slot) # Invalidate base indirection
        
        curr = latest        
        while curr not in (0, -1, base_rid, None): # Follow indirection chain to all tail rids
            self.invalidate_tail_rid(curr) # Use helper function to set tail rid to -1
            curr = self.read_tail_value(curr, INDIRECTION_COLUMN) # Update current tail rid

        return True
        

    def get_version_rid(self, base_rid, relative_version):
        # Use helper function to return rid at indirection col from base rid
        indirection = self.read_base_value(base_rid, INDIRECTION_COLUMN)
    
        # If no tail updates, returns base RID
        if indirection == 0 or indirection == -1:
            return base_rid

        # Loops through until it reaches the relative version
        curr = indirection
        for i in range(abs(relative_version)):
            previous_indirection = self.read_tail_value(curr, INDIRECTION_COLUMN) # Get previous indirection rid 
            if previous_indirection in (-1, 0, base_rid, None):
                # Account for deleted tails or only one tail 
                return base_rid # Account for deleted tail
            curr = previous_indirection

        return curr