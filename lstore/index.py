"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.table import Table
class Index:
    # One index for each table. All our empty initially.
    #key columns are set to index = 0 by default.

    def __init__(self, table):
        self.table = table
        self.indices = [None] *  table.num_columns

        

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        

  
  
  
  
  
  
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass
