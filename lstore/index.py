"""
A data strucutre holding indices for various columns of a table. Key column should be
indexd by default, other columns can be indexed through this object. Indices are
usually B-Trees, but other data structures can be used as well.
"""
from table import Table 
#need a function that retrieves primary key's index 
class Index:
    # Keeps per-column in-memory indices for a table.
    # `self.indices[i]` is either:
    # - None (no index built for column i), or
    # - a dict mapping column values -> list of RIDs.

    def __init__(self, table):
        # One index slot per user column; all start empty.
        self.table = table
        self.indices = [None] * table.num_columns
        # Key column is indexed by default (empty index to start).
        if hasattr(table, "key") and table.key is not None:
            self.indices[table.key] = {}

    # returns the location of all records with the given value on column "column"
    def locate(self, column, value):
        # Fast path for primary key if key_to_rid exists.
        key_map = getattr(self.table, "key_to_rid", None)
        if isinstance(key_map, dict) and column == self.table.key:
            # Primary key values are unique, so this returns at most one RID.
            rid = key_map.get(value)
            return [] if rid is None else [rid]

        # Use a built index if one exists for this column.
        index = self.indices[column]
        if index is not None:
            # Non-key columns can map one value to multiple matching rows.
            return list(index.get(value, []))
        # No index available for this column.
        return []
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    def locate_range(self, begin, end, column):
        # Fast path for primary key if key_to_rid exists.
        key_map = getattr(self.table, "key_to_rid", None)
        if isinstance(key_map, dict) and column == self.table.key:
            # Scan key->RID map and collect rows whose keys are in [begin, end].
            rids = []
            for key, rid in key_map.items():
                if begin <= key <= end:
                    rids.append(rid)
            return rids

        # Use a built index if one exists for this column.
        index = self.indices[column]
        if index is not None:
            # Scan indexed values and merge RID lists for values in range.
            rids = []
            for key, value_rids in index.items():
                if begin <= key <= end:
                    rids.extend(value_rids)
            return rids
        # No index available for this column.
        return []

    # optional: Create index on specific column
    def create_index(self, column_number):
        # Placeholder method.
        # Intended behavior: populate `self.indices[column_number]`
        # from existing table records.
        ind= {} 
        

        

    # optional: Drop index of specific column
    def drop_index(self, column_number):
        # Remove the index structure for this column.
        self.indices[column_number] = None
        
    # functions that are helpful

        
    def rid_column(self):    #create iterator to see rid and columns
        # Yields tuples of (RID, user_columns[]) by reading base pages row-wise.
        base_pd = self.table.base_pd
        base_columns = len(base_pd.pageDirectoryBase)
        if base_columns == 0:
            return  # No records

        # Convention used here: first base column stores RID values.
        ridC = base_pd.pageDirectoryBase[0]
        # Remaining base columns are user-visible data columns.
        otherC = base_pd.pageDirectoryBase[1:]

        num_records = ridC.num_records()
        for i in range(num_records):
            rid = ridC.get_value_at(i)
            columns = [col.get_value_at(i) for col in otherC]
            yield rid, columns
  

    #hashtbale for table.py
    def key_rid(self, table, key):
        # Safe helper for key lookup in table.key_to_rid.
        if table is None:
            return None
        key_to_rid = getattr(table, "key_to_rid", None) # mapping key values  to RIDs.
        if not isinstance(key_to_rid, dict): # If the mapping doesnt exist or isn't a dictionary, then return None.
            return None
        rid = key_to_rid.get(key)
        return rid
