class BaseFilter(object):
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data
        self.doquery = self.data.query_df
