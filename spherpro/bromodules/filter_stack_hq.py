"""
A class to generate and add a hq filter to the database.
this filter is a composition from diffrent silent (as in not saved to the db)
conditions.
"""
import spherpro.bromodules.filter_base as filter_base
import spherpro.bromodules.filter_customfilterstack as filter_objectfilters
import spherpro.bromodules.filter_measurements as filter_measurements
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa




class StackHQ(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.filter_custom = filter_objectfilters.CustomFilterStack(bro)
        self.filter_measurements = filter_measurements.FilterMeasurements(bro)
        self.doquery = self.data.get_query_function()

        self.col_issphere_issphere = 'is-sphere'
        self.col_isother_issphere = 'is-other'
        self.col_measure_issphere = 'MeanIntensity'
        self.col_stack_issphere = 'BinStack'
        self.outcol_issphere = 'is-sphere-out'
        self.non_zero_offset = 1/10000


        self.col_distother = "dist-other"
        self.col_measure = "MeanIntensity"
        self.col_stack = "DistStack"
        self.outcol_ambigous = "is-ambigous-out"

    def create(self, minfrac=0.01, distother=-5,
               filname=None, drop=False):
        """
        Creates the HQ filterdata and saves it to the db

        Args:
            minfrac (float): fraction of pixel that need to be in belong to the sphere in order to assign it
            to the sphere.
            distother (float): distance to the next neightbour (negative)
            filname (str): name of the filter. Default: is-hq
            drop (bool): overwrite existing filter with the same name? Default: False
        """
        if filname is None:
            filname = 'is-hq'

        col_issphere_issphere = self.col_issphere_issphere
        col_isother_issphere = self.col_isother_issphere
        col_measure_issphere = self.col_measure_issphere
        col_stack_issphere = self.col_stack_issphere
        outcol_issphere = self.outcol_issphere
        non_zero_offset = self.non_zero_offset

        # Check Cells to be spherical
            # get MI issphere
            # get MI isother
        issphere = self._get_joined_measurements(
           [{db.ref_planes.channel_name.key: cn,
             db.stacks.stack_name.key: col_stack_issphere,
             db.measurements.measurement_name: col_measure_issphere}
            for cn in (col_issphere_issphere, col_isother_issphere)
            ])
        issphere.loc[:, outcol_issphere] = (
            (((issphere[col_issphere_issphere]+non_zero_offset) >=
             (issphere[col_isother_issphere]+non_zero_offset))) &
            (issphere[col_issphere_issphere] >  minfrac))

        # Check if cells are on rim touching another spheroid
        # get MinI dist-other

        col_distother = self.col_distother
        col_measure = self.col_measure
        col_stack = self.col_stack
        outcol_ambigous = self.outcol_ambigous

        isambigous = self._get_joined_measurements([
            {db.ref_planes.channel_name.key: col_distother,
             db.stacks.stack_name.key: col_stack,
             db.measurements.measurement_name: col_measure}])
        isambigous.loc[:, outcol_ambigous] = (
            (
                isambigous[col_distother] > distother
            )
            &
            (
                isambigous[col_distother] < (2**16-2)
            )
        )
        data = isambigous.join(issphere)
        # HQ Filter
        data.loc[:, db.object_filters.filter_value.key] = (
            data[outcol_issphere] & (data[outcol_ambigous] == False)
        ).map(int)
        data = data.reset_index(drop=False)
        self.filter_custom.write_filter_to_db(data, filname, drop=drop)
        return data

    def _get_joined_measurements(self, measurement_dicts):
        """
        gets the joined tables according to the names in list
        """
        data_list = list()
        for mdict in measurement_dicts:
            cname = mdict[db.ref_planes.channel_name.key]
            fil = self.filter_measurements.get_measurement_filter_statements(
            *[[mdict.get(o,d)] for o, d in self.filter_measurements.measure_idx])
            base_query = (self.session.query(
                                    db.objects.object_id,
                                    db.object_measurements.value,
                                    db.ref_stacks.scale
                                )
                        .join(db.object_measurements)
                        .join(db.measurements)
                        .join(db.planes)
                        .join(db.ref_planes)
                        .join(db.ref_stacks))
            q  = (base_query.filter(fil))
            data = self.doquery(q)
            data.loc[:, cname] = data[db.object_measurements.value.key] * data[db.ref_stacks.scale.key]
            data = data.set_index(db.objects.object_id.key)
            data_list.append(data[cname])
        outdat = pd.DataFrame(data_list).T
        return outdat
