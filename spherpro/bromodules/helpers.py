import spherpro.bromodules.helpers_anndata as ha
import spherpro.bromodules.helpers_varia as hv


class Helpers(object):
    def __init__(self, bro):
        self.dbhelp = hv.HelperDb(bro)
        self.anndata = ha
