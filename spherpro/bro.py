import spherpro.bromodules.filters as filters
import spherpro.bromodules.helpers as helpers
import spherpro.bromodules.io as io
import spherpro.bromodules.plots as plots
import spherpro.bromodules.processing as processing
import spherpro.datastore as datastore
import spherpro.db as db


def get_bro(fn_config, readonly=True):
    """
    Convenience function to get a bro with a datastore initialized
    with a config file
    Args:
        fn_config: path to the config file
        readonly: load database readonly?
    Returns:
        A true bro
    """
    store = datastore.DataStore()
    store.read_config(fn_config)
    store.resume_data(readonly=readonly)
    bro = store.bro
    return bro


class Bro(object):
    """docstring for Bro."""

    def __init__(self, DataStore):
        self.data = DataStore
        self.helpers = helpers.Helpers(self)
        self.filters = filters.Filters(self)
        self.io = io.Io(self)
        self.plots = plots.Plots()
        self.plots.load_modules(self)
        self.processing = processing.Processing()
        self.processing.load_modules(self)
        self.doquery = self.data.query_df
        self.session = self.data.main_session

    @property
    def is_debarcoded(self):
        isdeb = False
        q = self.data.main_session.query(db.images.condition_id)
        q = q.filter(db.images.condition_id.isnot(None)).count()
        if q > 0:
            isdeb = True
        return isdeb
