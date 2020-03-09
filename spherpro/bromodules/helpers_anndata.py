import spherpro.db as db


class HelpersAnndata:
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data

def add_anndata_varmeta(ad, dat_meta, on=None, on_index=False):
    ad.var = _add_anndata_meta(ad.var, dat_meta, index_var=db.measurements.measurement_id.key,
                               on=on, on_index=on_index)
    return ad

def add_anndata_obsmeta(ad, dat_meta, on=None, on_index=False):
    ad.obs = _add_anndata_meta(ad.obs, dat_meta, index_var=db.objects.object_id.key,
                               on=on, on_index=on_index)
    return ad

def _add_anndata_meta(ad_meta, dat_meta, index_var, on=None, on_index=False):
    if (on_index) or ((on is None) and index_var in ad_meta.columns) or (on == index_var):
        ad_meta = _join_indexvar(ad_meta, dat_meta, index_var, on_index)
    else:
        ad_meta = _merge_col(ad_meta, dat_meta, on=on)
    return ad_meta

def _merge_col(admeta, dat, on=None):
    return (admeta.reset_index()
            .merge(dat, on=on)
            .set_index('index')).loc[admeta.index, :]

def _join_indexvar(admeta, dat, idxvar, on_index):
    if on_index is False:
        dat = dat.set_index(idxvar)
    dat.index = map(str, dat.index)
    return admeta.join(dat, how='left')
