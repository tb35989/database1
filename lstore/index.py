"""
A data strucutre holding indices for various columns of a table. Key column should be
indexd by default, other columns can be indexed through this object. Indices are
usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        #  One index for each table. All our empty initially.
        # Each column has unique index 
        # shoes None if a column is not indexed.
        self.indices = [None] * table.num_columns
        self.table = table
        if hasattr(table, "key") and table.key is not None:
            self.create_index(table.key)

    # returns the location of all records with the given value on column "column"
    def locate(self, column, value):
        # if table.key_to_rid is present, faster lookup.
        if self.rid_key() and column == self.table.key:
            rid = self.table.key_to_rid.get(value)
            return [] if rid is None else [rid]
       
        index = self.indices[column]   # check if a index already exists
        if index is not None:
            return list(index.get(value, []))
     
        pairs = []  # No index, do full scan
        for rid, columns in self.rid_column():
            if column < len(columns) and columns[column] == value:
                pairs.append(rid)
        return pairs
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    def locate_range(self, begin, end, column):
        ind = self.indices[column]
        pairs = []





      

    # optional: Create index on specific column
    def create_index(self, column_number):
        ind= {} 
        

        

    # optional: Drop index of specific column
    def drop_index(self, column_number):
        self.indices[column_number] = None
        
    # functions that are helpful
    def rid_key(self):
        return hasattr(self.table, "key_to_rid") and isinstance(self.table.key_to_rid, dict)

        
    def rid_column(self):    #create iterator to see rid and columns   
        base_pd = self.table.base_pd
        base_columns = len(base_pd.pageDirectoryBase)
        if base_columns == 0:
            return  # No records

        ridC = base_pd.pageDirectoryBase[0]
        otherC = base_pd.pageDirectoryBase[1:]

        num_records = ridC.num_records()
        for i in range(num_records):
            rid = ridC.get_value_at(i)
            columns = [col.get_value_at(i) for col in otherC]
            yield rid, columns
  

       
