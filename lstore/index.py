"""
A data strucutre holding indices for various columns of a table. Key column should be
indexd by default, other columns can be indexed through this object. Indices are
usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        #  One index for each table. All our empty initially.
        # Each column has unique index 
        # When a column is not indexed, its slot stays None.
        self.indices = [None] * table.num_columns
        self.table = table
        if hasattr(table, "key") and table.key is not None:
            self.create_index(table.key)

    # returns the location of all records with the given value on column "column"
    def locate(self, column, value):
        ind = self.indices[column]
        if ind is not None:
            return list(ind.get(value, []))
        pairs = []
        for rid, columns in self._iter_records():
            if column < len(columns) and columns[column] == value:
                pairs.append((rid, columns[column]))
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
    def add_record(self, rid, columns):


     def update_record(self, rid, old_columns, new_columns): 

    def remove_record(self, rid, columns):

        

  

       