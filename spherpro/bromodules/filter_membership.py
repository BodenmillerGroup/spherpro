import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re
import sqlalchemy as sa

import plotnine as gg

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import spherpro.bromodules.filter_customfilterstack as custfilter

CHANNEL_DISTSPHERE = 'dist-sphere'

class FilterMembership(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.filter_custom = custfilter.CustomFilterStack(bro)

    def add_issphere(self, minfrac=0.01, name=None, drop=True):
        if name is None:
            name = 'is-sphere'
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        non_zero_offset = 1/100000
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
                        [col_isother,col_issphere])))
        dat_filter[db.object_measurements.value.key] = (dat_filter[db.object_measurements.value.key] * dat_filter[db.ref_stacks.scale.key])
        dat_filter = dat_filter.pivot_table(values=db.object_measurements.value.key,
                               columns=[db.ref_planes.channel_name.key],
                                            index=db.objects.object_id.key)
        dat_filter =  pd.DataFrame.from_dict({outcol_issphere: (
            (dat_filter[col_issphere]+non_zero_offset)/(
                dat_filter[col_isother]+non_zero_offset) > minfrac).map(int)},
            orient='columns')
        dat_filter.columns.names = [db.object_filter_names.object_filter_name.key]
        dat_filter = dat_filter.stack()
        dat_filter.name = db.object_filters.filter_value.key
        dat_filter = dat_filter.reset_index(drop=False)
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_isambiguous(self, distother=10, name=None, drop=True):
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
