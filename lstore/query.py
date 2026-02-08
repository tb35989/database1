from lstore.table import Table, Record
from lstore.index import Index

#0 - RID
#1 - INDIRECTION

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
        rid_list = self.table.index.locate(self.table.key, primary_key)
        #returns False if no elements returned 
        if len(rid_list) == 0:
            return False
        rid = rid_list[0]
        #returns False if record has already been deleted 
        if rid == -1:
            return False 
        self.table.rid_invalidation(rid)
        #remove rid key from index 
        if hasattr(self.table, "key_to_rid"):
            self.table.key_to_rid.pop(primary_key, None)
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    #table.py
    def insert(self, *columns): 
        schema_encoding = '0' * self.table.num_columns
        return self.table.insert(*columns)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        #may return multiple rids depending on search key 
        rid_list = self.table.index.locate(search_key_index, search_key)
        recordObjects = []
        #returns False if no record exists in the given range
        if len(rid_list) == 0:
            return []
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue
            indirection = self.table.read_base_value(i, 1)
            cols = []
            #loops through the given projected columns to retrieve the corresponding values
            for index, j in enumerate(projected_columns_index):
                if j == 0:
                    cols.append(None)
                else:
                    #adjusting the index because the first 4 are metadata cols
                    location = 4 + index
                    #no updates - read base record's value; if there are updates, reads the tail record's values
                    if indirection == 0:
                        value = self.table.read_base_value(i, location)
                    else:
                        value = self.table.read_tail_value(indirection, location)
                    cols.append(value)
            #creating a Record object and appending to list 
            recordObjects.append(Record(rid = i, key = search_key, columns = cols))
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
        #locates all the RIDs with the matching search_key
        rid_list = self.table.index.locate(search_key_index, search_key)
        #returns False if no records are found or all are deleted
        if len(rid_list) == 0:
            return False 
        recordObjects = []
        for i in rid_list:
            #skips over deleted records 
            if i == -1:
                continue 
            #gets the RID of the relative version
            rid_ver = self.table.get_version_rid(i, relative_version)
            cols = []
            #skips over values not in the projected_columns_index
            for index, j in enumerate(projected_columns_index):
                if j == 0:
                    cols.append(None)  
                else:
                    #reads base record or tail record value depending on updates
                    location = 4 + index  
                    if rid_ver == i:
                        value = self.table.read_base_value(i, location)
                    else:
                        value = self.table.read_tail_value(rid_ver, location)
                    cols.append(value)
            recordObjects.append(Record(rid=i, key=search_key, columns=cols))
        return recordObjects


    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    #MAKE IT RUN FASTER
    def update(self, primary_key, *columns):
        #locates rid based on primary key
        rid_list = self.table.index.locate(self.table.key, primary_key)
        #returns False if rid does not exist or is deleted  
        if len(rid_list) == 0:
            return False 
        rid = rid_list[0]
        indirection = self.table.read_base_value(rid, 1)
        if indirection == 0:
            prev_rid = rid
        else:
            prev_rid = indirection 
        tail_rid = self.table.new_tail_rid()
        tail_record = self.table.make_tail_record(base_rid=rid, prev_rid=prev_rid, tail_rid=tail_rid, updated_cols=columns) 
        #need to make 
        self.table.page_range.write_tail_record(rid=tail_rid, record=tail_record, rid_col=0)
        #updating base record indirection to point to newest tail
        base_row = self.table.page_range.getBaseRow(rid)
        slot = base_row[0]
        indirection_page = base_row[2] #1 + indirection col
        indirection_page.writeAtSlot(tail_rid, slot)
        return True
   
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        summation = 0
        location = 4 + aggregate_column_index
        #returns a list of RIDs which are in the given range 
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        #returns False if no record exists in the given range
        if len(rid_list) == 0:
            return False
        #if all RIDs are -1 (indication that they are all deleted)
        if self.table.read_base_value(i, RID_COLUMN) == -1:
            continue
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue
                #checks the indirection, retrieves the latest value from either the base or tail record, sums over
            indirection = self.table.read_base_value(i, 1)
            if indirection == 0:
                value = self.table.read_base_value(i, location)
            else:
                value = self.table.read_tail_value(indirection, location)
            summation += value    
        #returns the sum
        return summation

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(rid_list) == 0:
            return False
        summation = 0
        location = 4 + aggregate_column_index
        for i in rid_list:
            #skips RIDS which are -1 (indication that these records are deleted)
            if i == -1:
                continue 
            #retrives the latest value according to the relative version and adds to summation
            rid_ver = self.table.get_version_rid(i, relative_version)
            if rid_ver == i:
                value = self.table.read_base_value(i, location)
            else:
                value = self.table.read_tail_value(rid_ver, location)
            summation += value
        return summation 
    
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
