"""
A class to generate filter queries from measurements
and save them into the database.
"""
import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class FilterMeasurements(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.measure_idx =[ # idx_name, default
            (db.KEY_OBJECTID, 'cell'),
            (db.KEY_CHANNEL_NAME, None),
            (db.KEY_STACKNAME, 'FullStack'),
            (db.KEY_MEASUREMENTNAME, 'MeanIntensity'),
            (db.KEY_MEASUREMENTTYPE, 'Intensity')]

    def get_filter_query(self, measurement_dict, logical_operator, treshold):
        """
        g
        """

        measure_query = self.data.get_measurement_query()
        filter_statement = self.get_measurement_filter_statements(*[[
            measurement_dict.get(o,d)] for o, d in self.measure_idx ])
        filter_statement = sa.and_(filter_statement,
                logical_operator(self.data._get_table_column(db.TABLE_MEASUREMENT,
                                                             db.KEY_VALUE),
                treshold))
        return filter_statement


    def get_multifilter_query(self, query_triplets):
        """
        Allows to filter based on a combination of measurement values.
        The measurements, logical comparison and treshold are defined in
        the query triplets:
        Args:
            query_triplets: a list of tuples with:
                (measurement_dict, logical_operator, treshold)
                These parameters are documented in get_filter_query.
        Returns:
            filter_statement: can be used in a filter operation
                fitlers on the keys: ObjectID, ImageNumber and ObjectNumber
        """
        filters = [self.get_filter_query(m, l, t) for m, l, t in query_triplets]
        meas_query = self.data.get_measurement_query()
        subquerys = [meas_query.filter(fil).subquery() for i, fil in
                     enumerate(filters)]
        combined_filter_query = self.session.query(db.Objects)
        for subquery in subquerys:
            combined_filter_query = combined_filter_query.filter(sa.and_(
                db.Objects.ObjectID == subquery.c.ObjectID,
                db.Objects.ImageNumber == subquery.c.ImageNumber,
                db.Objects.ObjectNumber == subquery.c.ObjectNumber))

        subquery_filter = combined_filter_query.subquery()
        filter_statement = sa.and_(
            db.Objects.ObjectID == subquery_filter.c.ObjectID,
            db.Objects.ImageNumber == subquery_filter.c.ImageNumber,
            db.Objects.ObjectNumber == subquery_filter.c.ObjectNumber)
        return filter_statement

    def get_measurement_filter_statements(self, object_ids, channel_names,
                                          stack_names, measurement_names, measurement_types):
        """
        Generates a filter expression to query for multiple channels, defined as channel_names,
        stack_names, measurement names and measurement types.

        Input:
            object_ids: list of object_ids
            channel_names: list of channel names
            stack_names: list of stack_names
            measurement_names: list of measurement_names
            measurement_types: list of measurement measurement_types
        Returns:
            A dataframes with the selected measurements
        """
        constraint_columns = [
                              (db.TABLE_OBJECT, db.KEY_OBJECTID),
                              (db.TABLE_REFPLANEMETA, db.KEY_CHANNEL_NAME),
                              (db.TABLE_PLANEMETA, db.KEY_STACKNAME),
                              (db.TABLE_MEASUREMENT, db.KEY_MEASUREMENTNAME),
                              (db.TABLE_MEASUREMENT, db.KEY_MEASUREMENTTYPE)]
        constraint_columns = [self.data._get_table_column(t, c) for t, c in
                              constraint_columns]

        constraints = [sa.and_(*[c == v for c, v in zip(constraint_columns,
                                                        values)])
                       for values in zip(object_ids, channel_names, stack_names, measurement_names,
                       measurement_types)]
        if len(constraints) > 1:
            measure_filter = sa.or_(*constraints)
        else:
            measure_filter = constraints[0]
        return measure_filter
