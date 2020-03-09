import spherpro.bromodules.helpers_varia as hv
import spherpro.bromodules.helpers_anndata as ha

class Helpers(object):
    def __init__(self, bro):
        self.dbhelp = hv.HelperDb(bro)
        self.anndata = ha
