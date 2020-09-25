"""
A class to handle Anndata as a backend
"""
import pathlib
import shutil
import time

import anndata as ad
import numpy as np
import pandas as pd

import spherpro.bromodules.io_base as io_base
import spherpro.db as db

SUFFIX_ANNDATA = ".h5ad"


def get_anndata_filename(conf: object, object_type: str):
    fn = pathlib.Path(conf["sqlite"]["db"]).parent / (object_type + SUFFIX_ANNDATA)
    return fn


def scale_anndata(adat, col_scale=db.ref_stacks.scale.key, inplace=True):
    if not inplace:
        adat = adat.copy()
    adat.X = (adat.X.T * adat.var[col_scale][:, None]).T
    return adat


def copy_in_memory(adat):
    """
    Copies an adat into memory
    Args:
        adat: an andata object

    Returns:
        a in-memory copy of the anndata opbject

    """
    return ad.AnnData(np.array(adat.X), obs=adat.obs, var=adat.var, varm=adat.varm)


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
        a = self._adat
        if (a is None) or (  # Check if the data has been already loaded
            len(a.obs.index) != a.shape[0]
        ) | (
            len(a.var.index) != a.shape[1]
        ):  # check if data consistent
            self._adat = ad.read_h5ad(self.filename, backed="r")
        return self._adat


class IoObjMeasurements:
    def __init__(self, bro):
        self.bro = bro
        self._anndatadict = dict()
        self.scale_anndata = scale_anndata

    def get_anndata(self, obj_type):
        anndat = self._anndatadict.get(obj_type, None)
        if anndat is None:
            anndat = IoAnnData(self.bro, obj_type)
            self._anndatadict[obj_type] = anndat
        return anndat.adat

    def get_measurements(
        self,
        dat_obj=None,
        dat_meas=None,
        *,
        measidx=None,
        objidx=None,
        object_type=None,
        q_meas=None,
        q_obj=None,
    ):
        if q_meas is not None:
            dat_meas = self.bro.doquery(q_meas)
        if q_obj is not None:
            dat_obj = self.bro.doquery(q_obj)

        if dat_meas is not None:
            dat_meas = dat_meas.sort_values(db.measurements.measurement_id.key)
            measids = list(map(str, dat_meas[db.measurements.measurement_id.key]))
            dat_meas.index = measids
        else:
            measids = set(map(str, sorted(measidx)))

        if dat_obj is not None:
            obsidx = list(map(str, dat_obj[db.objects.object_id.key]))
            dat_obj.index = obsidx
            dat_obj.index.name = None
            dat_obj = dat_obj.sort_values(db.objects.object_id.key)
            it = dat_obj.groupby(db.objects.object_type.key)
        else:

            class obs:
                index = list(map(str, sorted(objidx)))

            it = [(object_type, obs)]

        dats = []
        for objtype, grp in it:
            obsidx = grp.index
            adat = self.get_anndata(objtype)
            varidx = list(set(measids).intersection(adat.var.index))
            if (len(varidx) > 0) & (len(obsidx) > 0):
                adat = adat[:, sorted(varidx, key=int)]
                dats.append(
                    ad.AnnData(adat.X, obs=adat.obs, var=adat.var)[obsidx, :].copy()
                )
        if len(dats) == 1:
            dat = dats[0]
        elif len(dats) == 0:
            raise ValueError("No valid measurements found")
        else:
            dat = ad.AnnData.concatenate(*dats, join="outer", index_unique=None)
        if dat_meas is not None:
            dat.var = dat.var.join(dat_meas)
        if dat_obj is not None:
            dat.obs = dat.obs.join(dat_obj)
        return dat

    @staticmethod
    def convert_anndata_legacy(adat):
        d = pd.DataFrame(
            adat.X, index=adat.obs.object_id, columns=adat.var.measurement_id
        ).stack()
        d.name = db.object_measurements.value.key
        dat = d.reset_index().merge(adat.obs.reset_index(drop=True)).merge(adat.var)
        return dat

    def add_anndata_datmeasurements(self, dat_new, replace=True, drop_all_old=False):
        for obj_type, dat in dat_new.groupby(db.objects.object_type.key):
            adat_new = ad.AnnData(
                dat.pivot(
                    index=db.objects.object_id.key,
                    columns=db.measurements.measurement_id.key,
                    values=db.object_measurements.value.key,
                )
            )
            self.add_anndata_objectmeasurements(obj_type, adat_new)

    def add_anndata_objectmeasurements(
        self, obj_type, adat_new, replace=True, drop_all_old=True
    ):
        """
        Adds a measurments to the anndata
        Args:
            obj_type: the object type
            adat_new: the anndata to be added
            replace: should existing measurements be updates?
            drop_all_old: should existing measurements be completly droped
                          before updating?

        Returns:

        """
        adat = self.get_anndata(obj_type)
        # Check that no new objects were added
        if len(get_difference(adat_new.obs.index, adat.obs.index)) != 0:
            raise ValueError(
                "The new data contains new objects, which is not supported yet."
            )

        old_vars = get_overlap(adat.var.index, adat_new.var.index)
        if len(old_vars) > 0:
            if not replace:
                raise ValueError(
                    f"Measurements {old_vars} already existing"
                    f" but replace=False was set!.\n"
                    f"Set replace=True to update values."
                )
            if (not drop_all_old) and (adat.shape[0] != adat_new.shape[0]):
                raise ValueError(
                    "Updating of existing variables only allowed"
                    " if values for all observations"
                    " are provided  or 'drop_all_old=True'"
                )
            else:
                kvars = [i for i in adat.var.index if i not in old_vars]
                adat = adat[:, kvars]

        adat = copy_in_memory(adat)
        adat = adat.T.concatenate(
            adat_new.T, index_unique=None, batch_key="batch", join="outer"
        ).T
        adat.var = adat.var.drop(columns="batch")

        ioan = self._anndatadict[obj_type]
        ioan.adat.file.close()

        fn_backup = f'{ioan.filename}.{time.strftime("%Y%m%d-%H%M%S")}'
        shutil.move(str(ioan.filename), fn_backup)

        # check order of variables
        ordvars = sorted(adat.var.index, key=int)
        if ordvars != list(adat.var.index):
            adat = adat[:, ordvars]

        adat.write(str(ioan.filename))
        ioan._adat = None
        return adat


def get_overlap(a, b):
    sa = set(a)
    sb = set(b)
    return sa.intersection(sb)


def get_difference(a, b):
    sa = set(a)
    sb = set(b)
    return sa.difference(sb)
