import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re
import sqlalchemy as sa

import plotnine as gg

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.configuration as conf
import spherpro.db as db
import spherpro.bromodules.filter_objectfilters as custfilter
from spherpro.bromodules.helpers_varia import HelperDb
import spherpro.library as lib

class FilterMembership(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.filter_custom = custfilter.ObjectFilterLib(bro)
        self.defaults = self.data.conf[conf.QUERY_DEFAULTS]
        self.helperdb = HelperDb(bro)

    def add_issmall(self, minpix=10, name=None, measid_area=None,
                    object_type=None, drop=True):
        if name is None:
            name = 'is-small'
        obj_def = self.defaults[conf.OBJECT_DEFAULTS]
        measfilts = self.bro.filters.measurements

        if measid_area is None:
            fil = measfilts.get_measmeta_filter_statements(
                channel_names=[obj_def[conf.DEFAULT_CHANNEL_NAME]],
                stack_names=[obj_def[conf.DEFAULT_STACK_NAME]],
                measurement_names=['Area'],
                measurement_types=['AreaShape'])
            measid_area = (self.data.get_measmeta_query()
                           .filter(fil)
                           .with_entities(db.measurements.measurement_id)
                           ).one()[0]
        q_dat = (self.data.get_measurement_query()
               .filter(db.measurements.measurement_id == measid_area)
               )
        if object_type is not None:
            q_dat = q_dat.filter(db.objects.object_type == object_type)

        dat_filter = self.bro.doquery(q_dat)
        dat_filter[db.object_filters.filter_value.key] = \
            dat_filter[db.object_measurements.value.key] < minpix
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_ismaincomponent(self, name=None, drop=True,
                            relation='Neighbors',
                            obj_type='cell'):
        if name is None:
            name = 'is-maincomponent'
        dat_nb = self.helperdb.get_nb_dat(relation, obj_type=obj_type)
        dat_obj = self.bro.doquery(self.session.query(db.objects.object_id, db.objects.image_id)
                   .join(db.valid_objects)
                   .filter(db.objects.object_type == obj_type)
                   )
        largest_obj = (dat_nb
                       .merge(dat_obj, left_on=db.object_relations.object_id_parent.key,
                              right_on=db.objects.object_id.key)
                       .groupby(db.images.image_id.key)
                       .apply(lib.get_largest_commponent_objs)
                       )
        dat_obj[db.object_filters.filter_value.key] = \
            dat_obj[db.objects.object_id.key].isin(largest_obj)
        self.filter_custom.write_filter_to_db(dat_obj, name, drop)
        return dat_obj

    def add_issphere(self, minfrac=0.6, name=None, drop=True):
        if name is None:
            name = 'is-sphere'
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_isbg = 'is-bg'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        non_zero_offset = 1/2**20
        dat_filter = self.doquery(
                (self.session
                     .query(
                         db.ref_stacks.scale,
                         db.object_measurements.object_id,
                                 db.object_measurements.value,
                                 db.ref_planes.channel_name
                                   )
                    .join(db.ref_planes)
                    .join(db.planes)
                    .join(db.measurements)
                    .join(db.object_measurements)
                     )
                    .filter(db.measurements.measurement_name==col_measure)
                    .filter(db.stacks.stack_name==col_stack)
                    .filter(db.ref_planes.channel_name.in_(
                        [col_isother,col_issphere,col_isbg])))
        dat_filter[db.object_measurements.value.key] = (dat_filter[db.object_measurements.value.key] * dat_filter[db.ref_stacks.scale.key])
        dat_filter = dat_filter.pivot_table(values=db.object_measurements.value.key,
                               columns=[db.ref_planes.channel_name.key],
                                            index=db.objects.object_id.key)
        dat_filter =  pd.DataFrame.from_dict({outcol_issphere: (
            (dat_filter[col_issphere]+non_zero_offset)/(
                dat_filter[col_isother]+dat_filter[col_isbg]+dat_filter[col_issphere]+non_zero_offset) > minfrac).map(int)},
            orient='columns')
        dat_filter.columns.names = [db.object_filter_names.object_filter_name.key]
        dat_filter = dat_filter.stack()
        dat_filter.name = db.object_filters.filter_value.key
        dat_filter = dat_filter.reset_index(drop=False)
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_isambiguous(self, distother=-10, name=None, drop=True):
        if name is None:
            name = 'is-ambiguous'
        col_measure = 'MeanIntensity'
        col_stack = 'DistStack'
        col_distother = 'dist-other'
        dat_filter = self.doquery(
                (self.session
                     .query(
                         db.ref_stacks.scale,
                         db.object_measurements.object_id,
                                 db.object_measurements.value,
                                 db.ref_planes.channel_name
                                   )
                    .join(db.ref_planes)
                    .join(db.planes)
                    .join(db.measurements)
                    .join(db.object_measurements)
                     )
                    .filter(db.measurements.measurement_name==col_measure)
                    .filter(db.stacks.stack_name==col_stack)
                    .filter(db.ref_planes.channel_name == col_distother))
        dat_filter[db.object_measurements.value.key] = (dat_filter[db.object_measurements.value.key]
                                                        * dat_filter[db.ref_stacks.scale.key])
        dat_filter[db.object_filters.filter_value.key] = (
           (dat_filter[db.object_measurements.value.key] > distother) & (
           dat_filter[db.object_measurements.value.key] < (2**16-2))).map(int)

        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter
