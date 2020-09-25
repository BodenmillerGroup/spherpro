"""
A class to generate handle the loading of the stackimages specified in the database.
"""
import functools
import os

import numpy as np
import tifffile as tif

import spherpro.bromodules.io_base as io_base
import spherpro.configuration as conf
import spherpro.db as db

max_cache = 384

NCHANNEL_ERROR = "Image has incompatible number of channels."


class IoStackImage(io_base.BaseIo):
    def __init__(self, bro):
        super().__init__(bro)
        cpconf = self.data.conf[conf.CPOUTPUT]
        self.basedir = cpconf[conf.IMAGES_CSV][conf.STACKIMG_DIR]
        if self.basedir is None:
            self.basedir = self.data.conf[conf.CP_DIR]
        else:
            self.basedir = self.basedir.format(
                **{conf.CP_DIR: self.data.conf[conf.CP_DIR]}
            )
        self._dat_stackimgs = None

    @functools.lru_cache(maxsize=max_cache)
    def get_planeimg(self, image_id, plane_id):
        stack_id, plane_number = self._get_stackmeta_for_plane(plane_id)
        img = self.get_stackimg(image_id, stack_id)
        img_plane_number = plane_number - 1
        return img[img_plane_number, :, :]

    def get_stack_nchan(self, stack_id):
        nchan = (
            self.session.query(db.planes.plane_id)
            .join(db.stacks)
            .filter(db.stacks.stack_id == stack_id)
        ).count()
        return nchan

    def _get_stackmeta_for_plane(self, plane_id):
        stack_id, plane_number = (
            self.bro.session.query(
                db.planes.stack_id, db.planes.ref_plane_number
            ).filter(db.planes.plane_id == plane_id)
        ).one()
        return stack_id, plane_number

    @functools.lru_cache(maxsize=max_cache)
    def get_stackimg(self, image_id, stack_id=None):
        """
        Retrieves an image stack based on image & stack id
        Args:
            image_id: the image id
            stack_id: the stack id
        Keyword Args:
            stack_name: optionally accepted instead of stack_id
        Returns:
            image_array: a (memorymapped) image array
        """
        fn = self.get_stackimg_fn(image_id, stack_id)
        img = tif.imread(os.path.join(self.basedir, fn), out="memmap")
        imshape = img.shape
        nchan = self.get_stack_nchan(stack_id)

        if nchan == 1:
            img = img.squeeze()
            if len(img.shape) == 2:
                img = img.reshape([0] + list(img.shape))
            else:
                raise ValueError(NCHANNEL_ERROR)

        if imshape[0] == imshape[2]:
            if nchan < 4:
                last = True
            else:
                last = False
        elif imshape[2] == nchan:
            last = True
        elif imshape[0] == nchan:
            last = False
        elif nchan < 3:
            last = True

        else:
            raise ValueError(NCHANNEL_ERROR)
        if last:
            return np.rollaxis(img, 2)
        else:
            return img

    def get_stackimg_fn(self, image_id, stack_id=None, *, stack_name=None):
        q = self.bro.session.query(db.image_stacks.image_stack_filename).filter(
            db.image_stacks.image_id == image_id
        )
        if stack_id is not None:
            q = q.filter(db.image_stacks.stack_id == stack_id)
        elif stack_name is not None:
            q = q.join(db.stacks).filter(db.stacks.stack_name == stack_name)
        else:
            raise ValueError("Either stack_id or stack_name need to be provided.")
        fn = q.one()[0]
        return fn

    def clear_caches(self):
        self.get_stackimg.cache_clear()
