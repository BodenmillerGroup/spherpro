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


class ObjectFilterLib(filter_base.BaseFilter):
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
        fil_id = (self.data.main_session.query(db.object_filter_names.object_filter_id)
                      .filter(db.object_filter_names.object_filter_name == filtername).scalar())
        if fil_id is None:
            new_id = self.data._query_new_ids(db.object_filter_names.object_filter_id, 1)
            new_id = list(new_id)
            fil_id = new_id[0]
            dat = pd.DataFrame({db.object_filter_names.object_filter_name.key: [filtername],
                                db.object_filter_names.object_filter_id.key: new_id})
            self.data._add_generic_tuple(dat, db.object_filter_names)

        fil_id = int(fil_id)

        return fil_id

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
        filterdata = filterdata[[db.objects.object_id.key, db.object_filters.filter_value.key]]


        filterdata.loc[:,db.object_filter_names.object_filter_id.key] = [self.add_filtername(filtername)]

        filterdata = filterdata.dropna()
        self.data._add_generic_tuple(filterdata, db.object_filters, replace=drop)
