# I think eventually we can get rid of this (this is the old page range class)

from lstore.page import Page, BasePage, TailPage

class PageTemp:
    def __init__(self, num_columns):
        self.base_pages = [BasePage(num_columns)]
        self.tail_pages = [TailPage(num_columns)]
        self.current_base_index = 0 # keeps track of the writable column
        self.current_tail_index = 0

    # Adds user info into columns inside base page (for INSERT)
    def write_base_record(self, rid, record, rid_column):
        last_page = self.base_pages[-1]
        # Checks if there is space in base page
        if last_page.get_offset == 4096: 
            self.add_base_page()
            last_page.append_record(rid, record, rid_column)
        else:
            last_page.append_record(rid, record, rid_column) 
    
    # Used when previous base page is full
    def add_base_page(self):
        if len(self.base_pages) == 16: # might not be necessary since we already check key uniqueness 
            return False 
        else:
            b = BasePage()
            self.base_pages.append(b)
            self.current_base_index += 1

    # Adds user info into columns inside tail page (for UPDATE)
    def write_tail_record(self, rid, record, rid_column):
        last_page = self.tail_pages[-1]
        # Checks if there is space in tail page
        if last_page.get_offset == 4096: 
            self.add_tail_page()
            last_page.append_record(rid, record, rid_column)
        else:
            last_page.append_record(rid, record, rid_column) 
    
    # Used when previous tail page is full
    def add_tail_page(self):
        t = TailPage()
        self.tail_pages.append(t)
        self.current_tail_index += 1
        