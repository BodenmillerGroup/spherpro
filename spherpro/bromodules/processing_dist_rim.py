import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class CalculateDistRim(object):
    """docstring for CalculateDistRim."""
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data

    def include_dist_rim(self):
        """
        calculates the distance-to-rim and adds it to the datastore
        """
        dist_bg, dist_other = _get_dists()
        dist_rim = _calc_dist_rim(dist_bg, dist_other)


    def _get_dists(self):
        dist_bg_q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=["dist-bg"],
                             object_ids=['cell'],
                             stack_names=['DistStack'],
                             measurement_names=['MeanIntensity'],
                             measurement_types=['Intensity'],
                         ))
                    )
        dist_bg = pd.read_sql_query(dist_bg_q.statement, self.data.db_conn)

        dist_other_q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=["dist-sphere"],
                             object_ids=['cell'],
                             stack_names=['DistStack'],
                             measurement_names=['MeanIntensity'],
                             measurement_types=['Intensity'],
                         ))
                    )
        dist_other = pd.read_sql_query(dist_other_q.statement, self.data.db_conn)

        return dist_bg, dist_other


    def _calc_dist_rim(self, dist_bg, dist_other):
        pass
