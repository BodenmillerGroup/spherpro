import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import plotnine as gg


CHANNEL_DISTSPHERE = 'dist-sphere'

class FilterMembership(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)

    def add_issphere(self, minfrac=0.01):
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        non_zero_offset = 1/100000
        dat_filter = pd.read_sql(
                (self.session
                     .query(
                                 db.object_measurements.ImageNumber,
                                 db.object_measurements.ObjectID,
                                 db.object_measurements.ObjectNumber,
                                 db.object_measurements.value,
                                 db.ref_planes,
                                 db.ref_stacks.scale
                                   )
                    .filter(db.object_measurements.MeasurementName==col_measure)
                    .filter(db.object_measurements.StackName==col_stack)
                    .filter(db.ref_planes.channel_name.in_(
                        [col_isother,col_issphere]))
                    .join(db.planes)
                    .join(db.ref_planes)
                    .join(db.ref_stacks)
                     ).statement,
            self._conn)
        dat_filter[db.KEY_VALUE] = (dat_filter[db.KEY_VALUE] * dat_filter[db.KEY_SCALE])
        idx_cols = [c for c in dat_filter.columns
                    if c not in [db.KEY_VALUE, db.KEY_CHANNEL_NAME,
                                 db.KEY_PLANEID]]
        dat_filter = dat_filter.pivot_table(values=db.KEY_VALUE,
                               columns=[db.KEY_CHANNEL_NAME], index=idx_cols)
        dat_filter =  pd.DataFrame.from_dict({outcol_issphere: (
            (dat_filter[col_issphere]+non_zero_offset)/(
                dat_filter[col_isother]+non_zero_offset) > minfrac)},
            orient='columns')
        dat_filter.columns.names = [db.KEY_FILTERNAME]
        dat_filter = dat_filter.stack()
        dat_filter.name = db.KEY_FILTERVALUE
        dat_filter = dat_filter.reset_index(drop=False)
        dat_filter = dat_filter.loc[:,
                       self.bro.data._get_table_columnnames(db.TABLE_FILTERS)]
        query = self.session.query(db.object_filters).filter(db.object_filters.FilterName ==
                                                      outcol_issphere)
        table = self.bro.data._get_table_object(db.TABLE_FILTERS)
        self.bro.data._add_generic_tuple(dat_filter,
        query=query, table=table, replace=True)
        return dat_filter

    @property
    def _conn(self):
        return self.session.connection()
