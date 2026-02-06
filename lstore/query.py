from lstore.table import Table, Record
from lstore.index import Index

#helper functions needed for Query Class: read, delete (index), writing tail record, get_version_rid (table.py)
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        #returns the RID for a given primary_key value 
        rid = self.table.index.locate(self.table.key, primary_key)
        #returns False if record doesn't exist 
        if len(rid) == 0:
            return False
        #returns False if record has already been deleted
        if rid == -1:
            return False
        #returns the page and slot of the base record
        b_page, b_slot = self.table.base_pd.pageDirectoryBase[1].find(rid)
        b_index = self.table.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)

        #setting the base RID to a special value, -1 (invalidating)
        self.table.base_pd.pageDirectoryBase[1].connectedColumns[b_index].write(b_slot, -1)
        
        indirection = self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].read(b_slot)
        #checks whether record has been updated and changes the tail record's RID to a special value, -1
        if indirection != 0:
            t_rid = indirection
            t_page, t_slot = self.table.tail_pd.pageDirectoryTail[1].find(t_rid)
            t_index = self.table.tail_pd.pageDirectoryTail[1].connectedColumns.index(t_page)
            self.table.tail_pd.pageDirectoryTail[1].connectedColumns[t_index].write(t_slot, -1)

        #remove rid key from index 
        self.table.index.delete(self.table.key) #need to implement this function in index.py

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    #table.py
    def insert(self, *columns): 
        schema_encoding = '0' * self.table.num_columns
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    #EXTRACT PRIMARY KEY BASED ON THE RID INSTEAD OF USING SEARCH_KEY

    def select(self, search_key, search_key_index, projected_columns_index):
        #may return multiple rids depending on search key 
        rid_list = self.table.index.locate(search_key_index, search_key)
        recordObjects = []
        #returns False if no record exists in the given range
        if len(rid_list) == 0:
            return False
        #if all RIDs are -1 (indication that they are all deleted)
        if all(i == -1 for i in rid_list):
            return False
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue
            b_page, b_slot = self.table.base_pd.pageDirectoryBase[1].find(i)
            b_index = self.table.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)
            indirection = self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].read(b_slot)
            #if no updates exist, create a Record object using the given column's values and append to the list
            if indirection == 0:
                #EXTRACT PRIMARY KEY BASED ON THE RID INSTEAD OF USING SEARCH_KEY
                record = Record(rid = i, key = search_key, columns = [self.table.base_pd.pageDirectoryBase[j + 4].connectedColumns[b_index].read(b_slot)
                                                                      for j, include in enumerate(projected_columns_index) if include == 1])
                recordObjects.append(record)
            else:
                #access the tail record's values and create a Record object, append to the list
                t_rid = indirection
                t_page, t_slot = self.table.tail_pd.pageDirectoryTail[1].find(t_rid)
                t_index = self.table.tail_pd.pageDirectoryTail[1].connectedColumns.index(t_page)
                #SAME HERE 
                record = Record(rid = t_rid, key = search_key, columns = [self.table.tail_pd.pageDirectoryTail[j+4].connectedColumns[t_index].read(t_slot)
                                                                          for j, include in enumerate(projected_columns_index) if include == 1])
                recordObjects.append(record)
        return recordObjects 



    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        #indirection to point to latest RID for version 0; 
        #version -1: go to indirection of ver 0 - might have schema encoding flag so have to go back 1 more - only tail records without flag for schema encoding col can be considered as a version 
        #if null, keep going back until value is not null
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
        #UPDATE SCHEMA ENCODING COL AS WELL
    def update(self, primary_key, *columns):
        rid_list = self.table.index.locate(self.table.key, primary_key)
        #returns False if no records exist with given key or if record has been deleted
        if len(rid_list) == 0: 
            return False
        rid = rid_list[0]
        if rid == -1:
            return False
        b_page, b_slot = self.table.base_pr.pageRangeBase[1].find(rid) # RETURNS PAGE OBJ, SLOT ?? 
        b_index = self.table.base_pr.pageRangeBase[1].connectedColumns.index(b_page)
        indirection = self.table.read_base_value(base_rid, INDIRECTION_COLUMN) # USE HELPER FUNCTION FOR INDIRECTION
        if indirection == 0:
            #create new tail RID
            t_rid = self.new_tail_rid()
            #NEED TO UPDATE THE COLS OF THE NEW TAIL RECORD (WITH METADATA AND USER DATA) - IN PROGRESS
            record = self.make_tail_record(old_t_rid, new_t_rid, columns)
            #NEED TO WRITE THE TAIL RECORD TO THE TAIL PAGE DATA STRUCTURE 
            #update base record's indirection column
            self.table.base_pr.pageRangeBase[0].connectedColumns[b_index].write(b_slot, t_rid)
        else:
            old_t_rid = indirection
            new_t_rid = self.new_tail_rid()
            #creating new tail record where its indirection points to the previous version
            record = self.make_tail_record(old_t_rid, new_t_rid, columns) # have to update cols in table.py function
            #NEED TO WRITE THE TAIL RECORD TO THE TAIL PAGE DATA STRUCTURE 
            #updating the base record so its indirection points to the latest tail record
            self.table.base_pr.pageRangeBase[0].connectedColumns[b_index].write(b_slot, new_t_rid)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        #returns a list of RIDs which are in the given range 
        rid_list = self.index.locate_range(start_range, end_range, self.table.key)
        #returns False if no record exists in the given range
        if len(rid_list) == 0:
            return False
        #if all RIDs are -1 (indication that they are all deleted)
        if all(i == -1 for i in rid_list):
            return False
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue
            #returns the page and slot where the RID is stored in the base RID col (col 0)
            b_page, b_slot = self.table.base_pd.pageDirectoryBase[1].find(i)
            #returns which page index this RID was found in (can read from the same page index in the other cols)
            b_index = self.table.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)
            #does indirection value change in base col to indicate an update?
            indirection = self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].read(b_slot) #need read function
            if indirection == 0:
                #reads the value stored at the same slot of the same page index in the aggregate_column
                value = self.table.base_pd.pageDirectoryBase[aggregate_column_index + 4].connectedColumns[b_index].read(b_slot) #need a read function
                #adds this value to the sum
                sum += value
            else:
                t_rid = indirection
                #index - rid - page directory - come to the record and see indirection has been updated - follow indirection for tail record
                #index to rid to page directory to base page to base record 
                #after base record, read indirection, if not 0, follow indirection pointer to read tail record (indirection holds rid of tail record) - page directory
                t_page, t_slot = self.table.tail_pd.pageDirectoryTail[1].find(t_rid)
                t_index = self.table.tail_pd.pageDirectoryTail[1].connectedColumns.index(t_page)
                value = self.table.tail_pd.pageDirectoryTail[aggregate_column_index + 4].connectedColumns[t_index].read(t_slot)
                sum += value
        return sum

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    #NEED TO CHANGE - IMPLEMENTING HELPER FUNCTION IN TABLE.PY
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        #can sum over the versions based on what it returns?
        #ver 0: latest, second last: -1, third last: -2
        sum = 0
        #returns a list of RIDs which are in the given range
        rid_list = self.index.locate_range(start_range, end_range, self.table.key)
        #returns False if no record exists in the given range
        if len(rid_list) == 0:
            return False
        #if all RIDs are -1 (indication that they are all deleted)
        if all(i == -1 for i in rid_list):
            return False
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue 
            #returns the page and slot where the RID is stored in the base RID col (col 0)
            b_page, b_slot = self.table.base_pd.pageDirectoryBase[1].find(i)
            #returns which page index this RID was found in (can read from the same page index in the other cols)
            b_index = self.table.base_pd.pageDirectoryBase[1].connectedColumns.index(b_page)

            #can i call on the sum function itself when relative_function = 0 and return 

            #sets the current rid and current version counters 
            curr_rid = i
            curr_version = 0
            #loops through the indirection until it reaches the given relative_version
            while curr_version < relative_version:
                #indirection points to the latest tail RID or to 0 if there were no updates
                indirection = self.table.base_pd.pageDirectoryBase[0].connectedColumns[b_index].read(b_slot)
                #no tail records exist if indirection equals 0
                if indirection == 0:
                    break
                else:   
                    #moves to the tail record's RID, as indicated by the indirection value
                    t_rid = indirection
                    ##returns the page and slot where the tail RID is stored
                    t_page, t_slot = self.table.tail_pd.pageDirectoryTail[1].find(t_rid)
                    #returns which page index this tail RID was found in (can read from the same page index in the other cols)
                    t_index = self.table.tail_pd.pageDirectoryTail[1].connectedColumns.index(t_page)
                    #overwrite the base record's pointers with the tail record's pointers so next loop starts from the tail record 
                    #Base → Tail1 → Tail2 → Tail3 → ... → latest
                    curr_rid = t_rid
                    b_page, b_slot, b_index = t_page, t_slot, t_index
                    #increase the current version
                    curr_version -= 1
            #if there were no updates to the base record, take the original value
            if curr_version == 0:
                value = self.table.base_pd.pageDirectoryBase[aggregate_column_index + 4].connectedColumns[b_index].read(b_slot)
            else:
                #takes the tail record's value if there was an update (as per the current_version)
                value = self.table.tail_pd.pageDirectoryTail[aggregate_column_index + 4].connectedColumns[b_index].read(b_slot)
            #adds the value to the total sum
            sum += value
        return sum
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
