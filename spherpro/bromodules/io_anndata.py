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

    def initialize_anndata(self, data):
        adat = ad.AnnData(data)
        adat.write(self.filename)

    @property
    def adat(self):
        if self._adat is None:
            self._adat = ad.AnnData(filename=self.filename,
                                         filemode='r')
        return self._adat




