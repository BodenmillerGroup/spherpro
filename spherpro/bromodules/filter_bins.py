"""
A class to generate and add a hq filter to the database.
this filter is a composition from diffrent silent (as in not saved to the db)
conditions.
"""
import spherpro.bromodules.filter_base as filter_base
import spherpro.bromodules.filter_customfilterstack as filter_customfilterstack
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa




class Bins(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.custfilter = filter_customfilterstack.CustomFilterStack(bro)

    def create(self, binwidth=10):
        """
        Creates a filter containing the bin number of the object
        """

        dist = self._get_joined_measurements([(col_stack,col_measure,col_distother)])


    def _get_joined_filtered_measurements(self, mdicts, filter, filtertuple):
        """
        gets the joined tables according to the names in list
        """
        sname, mname, cname = mdicts.pop(0)
        q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=[cname],
                             object_ids=['cell'],
                             stack_names=[sname],
                             measurement_names=[mname],
                             measurement_types=['Intensity'],
                         ))
                    )
        data = pd.read_sql_query(q.statement,self.data.db_conn)
        data = data.set_index([db.images.image_id.key, db.objects.object_number.key])
        data[cname] = data[db.object_measurements.value.key]
        data = pd.DataFrame(data[cname])
        for group in mdicts:
            sname, mname, cname = group
            q  = (self.data.get_measurement_query()
                         .filter(
                             self.bro.filters.measurements.get_measurement_filter_statements(
                                 channel_names=[cname],
                                 object_ids=['cell'],
                                 stack_names=[sname],
                                 measurement_names=[mname],
                                 measurement_types=['Intensity'],
                             ))
                        )
            tmp = pd.read_sql_query(q.statement,self.data.db_conn)
            tmp = tmp.set_index([db.images.image_id.key, db.objects.object_number.key])
            tmp[cname] = tmp[db.object_measurements.value.key]
            tmp = pd.DataFrame(tmp[cname])
            data = data.join(tmp)
        return data







