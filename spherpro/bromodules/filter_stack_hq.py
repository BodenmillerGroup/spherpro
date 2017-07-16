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




class StackHQ(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.custfilter = filter_customfilterstack.CustomFilterStack(bro)

    def create(self, minfrac=0.01):
        """
        Creates the HQ filterdata and saves it to the db
        """
        col_issphere_issphere = 'is-sphere'
        col_isother_issphere = 'is-other'
        col_measure_issphere = 'MeanIntensity'
        col_stack_issphere = 'BinStack'
        outcol_issphere = 'is-sphere-out'
        non_zero_offset = 1/10000

        # Check Cells to be spherical
            # get MI issphere
            # get MI isother
        issphere = self._get_joined_measurements([(col_stack_issphere,col_measure_issphere,col_issphere_issphere),(col_stack_issphere,col_measure_issphere,col_isother_issphere)])
        issphere[outcol_issphere] = pd.DataFrame(
            (
                (
                    issphere[col_issphere_issphere]+non_zero_offset
                )
                /
                (
                    issphere[col_isother_issphere]+non_zero_offset
                ) > minfrac
            )
        )
        # Check if cells are on rim touching another spheroid
            # get MinI dist-other
        col_distother = "dist-other"
        col_measure = "MinIntensity"
        col_stack = "DistStack"
        outcol_ambigous = "is-ambigous-out"
        isambigous = self._get_joined_measurements([(col_stack,col_measure,col_distother)])
        isambigous[outcol_ambigous] = pd.DataFrame(
            (
                isambigous[col_distother] > -5
            )
            &
            (
                isambigous[col_distother] == (2**16)
            )
        )
        data = self.custfilter._get_valueless_table()
        data = data.set_index([db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER])
        data = data.join(isambigous).join(issphere)
        data = data.reset_index(drop=False)
        # HQ Filter
        data[db.KEY_VALUE] = pd.DataFrame(
            data[outcol_issphere] & (data[outcol_ambigous] == False)
        )
        self.custfilter.write_filter_to_db(data, "is-hq")
        return data

    def _get_joined_measurements(self, mdicts):
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
        data = data.set_index([db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER])
        data[cname] = data[db.KEY_VALUE]
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
            tmp = tmp.set_index([db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER])
            tmp[cname] = tmp[db.KEY_VALUE]
            tmp = pd.DataFrame(tmp[cname])
            data = data.join(tmp)
        return data

