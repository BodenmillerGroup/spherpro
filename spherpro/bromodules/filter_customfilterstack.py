"""
A class to generate and add filters to the filter table.
"""
import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

FILTERSTACKNAME = "FilterStack"
FILTERTYPENAME = "filter"


class CustomFilterStack(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)

    def _get_valueless_table(self):
        """
        Used to create a Valueless Table. The Table represents a db.object_measurements
        table, missing the value, MEasurementType, MeasurementName and PlaneID-columns
        """
        q = self.data.main_session.query(db.objects)
        objects = pd.read_sql_query(q.statement, self.data.db_conn)
        objects[db.stacks.stack_name.key] = FILTERSTACKNAME
        return objects

    def add_filtername(self, filtername):
        """
        Adds a filtername to the objectfilter
        Args:
            filtername: a string
        """
        is_present = (self.data.main_session.query(sa.exists())
                      .where(db.object_filter_names.object_filter_name == filtername).scalar())

    def write_filter_to_db(self, filterdata, filtername, drop=None):
        """
        Writes a dataframe containing Filterdata to the DB.
        Args:
            filterdata: DataFrame containing the filterdata. needs to be in the
                        format returned by _get_valueless_table, just with an
                        added value table.
            filtername: String stating the Filtername
        """
        if drop is None:
            drop=False
        filterdata = filterdata[[db.objects.object_id, db.object_filters.filter_value.key]]


        filterdata[db.object_filter_names.object_filter_name.key] = filtername

        # filterdata[db.stacks.stack_name.key] = FILTERSTACKNAME
        filterdata = filterdata.dropna()
        self.data._add_generic_tuple(filterdata, db.object_filters, replace=drop)
