import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa


DEFAULT_RELATION = 'Neighbors'
DEFAULT_MEASURETYPE = 'Intensity'
VALUE = db.object_measurements.value.key
MEAS_ID = db.measurements.measurement_id.key
CHILD_ID = db.object_relations.object_id_child.key
PARENT_ID = db.object_relations.object_id_parent.key
OBJ_ID = db.objects.object_id.key
OLD_ID = 'oldid'

class AggregateNeightbours(object):
    """docstring for CalculateDistRim."""
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data
        self.mm = self.bro.processing.measurement_maker
        self._register_measurement_name = self.mm.register_measurement_name
        self._register_measurement_type = self.mm.register_measurement_type


    def add_nb_measurement(self, nb_meas_prefix, nb_agg_fkt,
                        nb_relationtype=None,
        object_type=None, measurement_name=None,
        stack_name=None, plane_id=None, measurement_type=None,
        filter_query=None, filter_statement=None, image_id=None):
        if nb_relationtype is None:
            nb_relationtype = DEFAULT_RELATION
        nb_dic_dat = self.get_nb_dat(nb_relationtype, filter_query)
        dat = self._get_data(
                object_type, measurement_name,
                stack_name, plane_id, measurement_type,
                  filter_query, filter_statement, image_id)
        fil = ((nb_dic_dat[CHILD_ID].isin(dat[OBJ_ID])) &
               (nb_dic_dat[PARENT_ID].isin(dat[OBJ_ID])) &
               (nb_dic_dat[PARENT_ID] != nb_dic_dat[CHILD_ID]))
        nb_dic_dat = nb_dic_dat.loc[fil, :]
        nb_dict =self._gen_nb_dict(nb_dic_dat)
        dat = dat.loc[dat[OBJ_ID].isin(nb_dic_dat[PARENT_ID]), :]
        nb_dat = self.agg_data(dat, nb_dict, nb_agg_fkt)
        old_ids = [int(i) for i in dat[MEAS_ID].unique()]
        id_dict = self.update_measurement_ids(old_ids, nb_meas_prefix)
        nb_dat[MEAS_ID] = nb_dat[MEAS_ID].replace(id_dict)
        self.data._add_generic_tuple(nb_dat, db.object_measurements, replace=True, pg=True)

    def agg_data(self, data, nb_dict, fkt):
        tdat = data.pivot(index=OBJ_ID, columns=MEAS_ID, values=VALUE)
        nb_dat = tdat.apply(self._agg_nb_val, axis=1, nbdict=nb_dict, data=tdat, fkt=fkt)
        nb_dat = nb_dat.stack()
        nb_dat.name = VALUE
        nb_dat = nb_dat.reset_index(drop=False)
        return nb_dat


    def get_nb_dat(self, relationtype_name, obj_type=None, fil_query=None):
        nbquery = (self.session.query(db.object_relations.object_id_parent,
                                      db.object_relations.object_id_child)
           .join(db.object_relation_types)
           .filter(db.object_relation_types.object_relationtype_name == relationtype_name)
                   )
        if obj_type is not None:
            nbquery = (nbquery
                       .join(db.objects,
                             db.objects.object_id == db.object_relations.object_id_parent)
                       .filter(db.objects.object_type == obj_type))
        if fil_query is not None:
            q_fil = fil_query.alias()
            nbquery = nbquery.filter(db.object_relations.object_id_child == fil_query.c.object_id,
                db.object_relations.object_id_parent == q_fil.c.object_id)
        return self.bro.doquery(nbquery)

    def _get_data(self, object_type=None, measurement_name=None,
            stack_name=None, plane_id=None, measurement_type=None,
                  filter_query=None, filter_statement=None, image_id=None):
        q = (self.session.query(db.object_measurements.value, db.object_measurements.measurement_id,
                           db.object_measurements.object_id)
              .join(db.measurements)
              .join(db.planes)
              .join(db.stacks)
             .join(db.objects)
              .join(db.images))
        if object_type is not None:
            q = q.filter(db.objects.object_type == object_type)
        if measurement_name is not None:
            q = q.filter(db.measurements.measurement_name == measurement_name)
        if stack_name is not None:
            q = q.filter(db.stacks.stack_name == stack_name)
        if filter_query is not None:
            q = q.filter(db.objects.object_id == filter_query.c.object_id)
        if filter_statement is not None:
            q = q.filter(filter_statement)
        if plane_id is not None:
            q = q.filter(db.planes.plane_id == plane_id)
        if image_id is not None:
            q = q.filter(db.images.image_id == image_id)
        return self.bro.doquery(q)

    @staticmethod
    def _agg_nb_val(row, data, nbdict, fkt):
        objid = row.name
        y = data.loc[list(nbdict[objid]), :].apply(fkt, axis=0, raw=True)
        return y

    @staticmethod
    def _gen_nb_dict(coldat):
        """
        Converts relationship lists to dict
        """
        first_obj =coldat[PARENT_ID].values
        second_obj=coldat[CHILD_ID].values
        nb_dict = dict((obj, set()) for obj in np.unique(first_obj))
        for f, s in zip(first_obj, second_obj):
            nb_dict[f].add(s)
        return nb_dict

    def update_measurement_ids(self, old_ids, meas_name_prefix):
        measure_meta = self.bro.doquery(self.session.query(db.measurements)
                        .filter(db.measurements.measurement_id.in_(
                            old_ids)))
        dic_meas_name = dict()
        for m in measure_meta[db.measurements.measurement_name.key].unique():
            m_new = meas_name_prefix + m
            dic_meas_name.update({m: m_new})
            self._register_measurement_name(m_new)
        measure_meta[db.measurements.measurement_name.key] = \
                measure_meta[db.measurements.measurement_name.key].replace(dic_meas_name)
        measure_meta = measure_meta.rename(columns={MEAS_ID: OLD_ID})
        measure_meta = self.mm.register_measurements(meaure_meta)
        id_dict = {old: new for old, new in zip(measure_meta[OLD_ID], measure_meta[MEAS_ID])
                    if old != new}
        return id_dict

