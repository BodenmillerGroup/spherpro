"""
A class to generate handle the loading of the mask specified in the database.
"""
import functools
import os
import re

import imctools.io.imcacquisition as imcacquisition
import imctools.io.ometiffparser as omepars
import numpy as np

import spherpro.bromodules.io_base as io_base
import spherpro.configuration as conf
import spherpro.db as db

max_cache = 384


class IoImc(io_base.BaseIo):
    def __init__(self, bro):
        super().__init__(bro)
        imgconf = self.data.conf[conf.CPOUTPUT][conf.IMAGES_CSV]

        self.ome_dirs = imgconf[conf.IMAGE_OME_FOLDER_DIRS]
        self.meta_re = re.compile(imgconf[conf.IMAGE_OME_META_REGEXP])
        self._ome_folddict = None

    @functools.lru_cache(maxsize=max_cache)
    def get_imc_acquisition(self, slideac_name, acid):
        """
        Retrieves masks based on a folder and acquisition_id
        Args:
            folder
        Returns:
            memmapped imcacquisition
        """
        fn_img = self.ome_folddict[slideac_name][acid]
        return omepars.OmetiffParser(fn_img).get_imc_acquisition()

    def get_imcimg(self, img_id):
        meta = self._get_imgmeta(img_id)
        slideac_name = meta[db.slideacs.slideac_name.key]
        acid = meta[db.acquisitions.acquisition_mcd_acid.key]
        imcac = self.get_imc_acquisition(slideac_name, acid)
        posx = int(meta[db.images.image_pos_x.key])
        posy = int(meta[db.images.image_pos_y.key])
        w = int(meta[db.images.image_shape_w.key])
        h = int(meta[db.images.image_shape_h.key])
        sl = np.s_[:, posx : (h + posx), posy : (w + posy)]
        cutac = imcacquisition.ImcAcquisition(
            imcac.image_ID,
            imcac.original_file,
            imcac.data[sl],
            imcac._channel_metals,
            imcac._channel_labels,
        )
        cutac.original_imcac = imcac
        return cutac

    def clear_caches(self):
        self.get_imc_acquisition.cache_clear()
        self._get_imgmeta.cache_clear()

    @functools.lru_cache(maxsize=max_cache)
    def _get_imgmeta(self, imgid):
        q = (
            self.data.main_session.query(
                db.images.image_id,
                db.images.image_pos_x,
                db.images.image_pos_y,
                db.images.image_shape_w,
                db.images.image_shape_h,
                db.acquisitions.acquisition_mcd_acid,
                db.slideacs.slideac_name,
            )
            .join(db.acquisitions, db.sites, db.slideacs)
            .filter(db.images.image_id == imgid)
        )
        r = {
            l: v
            for r in [q.one_or_none()]
            if r is not None
            for l, v in zip(r.keys(), r)
        }
        return r

    @property
    def ome_folddict(self):
        if self._ome_folddict is None:
            self._ome_folddict = {
                subfol: self._get_acs_from_fol(os.path.join(fol, subfol))
                for fol in self.ome_dirs
                for subfol in os.listdir(fol)
                if os.path.isdir(os.path.join(fol, subfol))
            }
        return self._ome_folddict

    def _get_acs_from_fol(self, fol):
        acdict = {}
        for fn in os.listdir(fol):
            m = self.meta_re.match(fn)
            if m is not None:
                acid = int(m.groupdict()[db.acquisitions.acquisition_mcd_acid.key])
                acdict[acid] = os.path.join(fol, fn)
        return acdict
