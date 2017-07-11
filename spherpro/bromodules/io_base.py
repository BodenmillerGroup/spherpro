class BaseIo(object):
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data
