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

def scale_anndata(adat, col_scale=db.ref_stacks.scale.key, inplace=True):
    if ~inplace:
        adat = adat.copy()
    adat.X = (adat.X.T * adat.var[col_scale][:, None]).T
    return adat

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
        self._anndatdict = dict()
        self.scale_anndata = scale_anndata

    def get_anndat(self, obj_type):
        anndat = self._anndatdict.get(obj_type, None)
        if anndat is None:
            anndat = IoAnnData(self.bro, obj_type)
            self._anndatdict[obj_type] = anndat
        return anndat.adat

    def get_measurements(self, dat_obj=None, dat_meas=None, *, measidx=None, objidx=None, object_type=None):
        if dat_meas is not None:
            dat_meas = dat_meas.sort_values(db.measurements.measurement_id.key)
            measids = list(map(str, dat_meas[db.measurements.measurement_id.key]))
            dat_meas.index = measids
        else:
            measids = set(map(str, sorted(measidx)))

        if dat_obj is not None:
            dat_obj = dat_obj.sort_values(db.objects.object_id.key)
            obsidx = list(map(str, dat_obj[db.objects.object_id.key]))
            dat_obj.index = obsidx
            it = dat_obj.groupby(db.objects.object_type.key)
        else:
            class obs:
                index = list(map(str, sorted(objidx)))
            it = [(object_type, obs)]

        dats = []
        for objtype, grp in it:
            obsidx = grp.index
            adat = self.get_anndat(objtype)
            varidx = list(set(measids).intersection(adat.var.index))
            if (len(varidx) > 0) & (len(obsidx) > 0):
                adat = adat[:, sorted(varidx)]
                dats.append(ad.AnnData(adat.X, obs=adat.obs, var=adat.var)[obsidx,:].copy())
        if len(dats) == 1:
            dat = dats[0]
        elif len(dats) == 0:
            raise ValueError('No valid measurements found')
        else:
            dat = ad.AnnData.concatenate(*dats, join='outer',index_unique=None)
        if dat_meas is not None:
            dat.var = dat.var.join(dat_meas)
        if dat_obj is not None:
            dat.obs = dat.obs.join(dat_obj)
        return dat
