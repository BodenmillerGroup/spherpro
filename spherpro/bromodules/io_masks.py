"""
A class to generate handle the loading of the mask specified in the database.
"""
import spherpro.bromodules.io_base as io_base
import spherpro.configuration as conf
import pandas as pd
import numpy as np
import re
import os

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import tifffile as tif

import functools

max_cache = 384

class IoMasks(io_base.BaseIo):
    def __init__(self, bro):
        super().__init__(bro)
        cpconf = self.data.conf[conf.CPOUTPUT]
        self.basedir = cpconf[conf.IMAGES_CSV][conf.MASK_DIR]
        if self.basedir is None:
            self.basedir = self.data.conf[conf.CP_DIR]

        self._dat_masks = None
        
    @functools.lru_cache(maxsize=max_cache)
    def get_mask(self, image_number):
        """
        Retrieves masks based on an image_number
        Args:
            image_number: Image number
        Returns:
            mask_array: numpy array with the mask labels as integer image
        """
        fn = self.dat_masks.loc[self.dat_masks[db.KEY_IMAGENUMBER] == image_number,
                           db.KEY_FILENAME].iloc[0]
        return tif.imread(os.path.join(self.basedir, fn))

    def clear_caches(self):
        self.get_mask.cache_clear()
    
    @property
    def dat_masks(self):
        if self._dat_masks is None:
           self._dat_masks = self.data.get_table_data(db.TABLE_MASKS)
        return self._dat_masks
