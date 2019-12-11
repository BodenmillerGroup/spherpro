"""
A class to handle Anndata as a backend
"""
import spherpro.bromodules.io_base as io_base
import spherpro.configuration as conf
import anndata as ad
import pandas as pd
import pathlib
import numpy as np
import re
import os

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

SUFFIX_ANNDATA = '.h5ad'

def get_anndata_filename(conf: object, object_type: str):
    fn = pathlib.Path(conf['sqlite']['db']).parent / (object_type + SUFFIX_ANNDATA)
    return fn

class IoAnnData(io_base.BaseIo):
    def __init__(self, bro, obj_type):
        super().__init__(bro)
        self.obj_type = obj_type
        self._adat = None

    @property
    def filename(self):
        """
        Filename of the file backing the anndata frame
        """
        return get_anndata_filename(self.bro.data.conf, self.obj_type)

    def initialize_anndata(self, data, var=None, obs=None):
        adat = ad.AnnData(data, var=var, obs=obs)
        adat.write(self.filename)

    @property
    def adat(self):
        if self._adat is None:
            self._adat = ad.AnnData(filename=self.filename,
                                         filemode='r')
        return self._adat

class IoObjMeasurements:
    def __init__(self, bro):
        self.bro = bro
        self.anndatdict = dict()

    def get_anndat(self, obj_type):
        anndat = self.anndatdict.get(obj_type, None)
        if anndat is None:
            anndat = IoAnnData(bro, obj_type)
            self.anndatdict[obj_type] = anndat
        return anndat.adat

    def get_measurements(self, dat_obj, dat_meas):
        measids = set(map(str, dat_meas[db.measurements.measurement_id.key]))
        dats = []
        for objtype, grp in dat_obj.groupby(db.objects.object_type.key):
            adat = self.get_anndat(objtype)
            obsidx = list(map(str, grp[db.objects.object_id.key]))
            varidx = list(measids.intersection(adat.var.index))
            if (len(varidx) > 0) & (len(obsidx) > 0):
                dats.append(anndata.AnnData(adat[obsidx, :].X, obs=adat.obs.loc[obsidx,:], var=adat.var)[:,varidx].copy())
                print(dats[-1].shape)
        if len(dats) == 1:
            return dats[0]
        else:
            return anndata.AnnData.concatenate(*dats, join='outer',index_unique=None)
