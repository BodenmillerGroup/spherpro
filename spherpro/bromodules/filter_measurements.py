"""
A class to generate filter queries from measurements
and save them into the database.
"""
import re
import operator
import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class FilterMeasurements(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.measure_idx =[ # idx_name, default
            (db.objects.object_id.key, 'cell'),
            (db.ref_planes.channel_name.key, None),
            (db.stacks.stack_name.key, 'FullStack'),
            (db.measurement_names.measurement_name.key, 'MeanIntensity'),
            (db.measurement_types.measurement_type.key, 'Intensity')]


    def get_filter_statement(self, measurement_dict, logical_operator,
                             treshold):
        """
        Gets a filter statement that can select objects by a certain value in a
        measurement.

        Args:
            measurement_dict: contains the indexes of the measurements
            logical_operator:
                from the 'operator' module: e.g. operator.eq, operator.lt,
                operator.gt
            treshold: a treshold to be used together with the logical operator
        Returns:
            A statement that can be used for filtering for ObjectID,
            ImageNumber and ObjectNumber
        """
        query_triplet = [(measurement_dict, logical_operator, treshold)]
        return self.get_multifilter_statement(query_triplet)

    def _get_filter_statement(self, measurement_dict, logical_operator, treshold):
        """
        NEVER USE THIS ALONE BUT JUST THROUGH GET MULTIFILTER STATEMENT
        """

        measure_query = self.data.get_measurement_query()
        filter_statement = self.get_measurement_filter_statements(*[[
            measurement_dict.get(o,d)] for o, d in self.measure_idx ])
        filter_statement = sa.and_(filter_statement,
                logical_operator(self.data._get_table_column(db.object_measurements.__tablename__,
                                                             db.object_measurements.value.key),
                treshold))
        return filter_statement


    def get_multifilter_statement(self, query_triplets):
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
        filters = [self._get_filter_statement(m, l, t) for m, l, t in query_triplets]
        meas_query = self.data.get_measurement_query()
        subquerys = [meas_query.filter(fil).subquery() for i, fil in
                     enumerate(filters)]
        combined_filter_query = self.session.query(db.objects)
        for subquery in subquerys:
            combined_filter_query = combined_filter_query.filter(sa.and_(
                db.objects.object_type == subquery.c.ObjectID,
                db.objects.image_id == subquery.c.ImageNumber,
                db.objects.object_number == subquery.c.ObjectNumber))

        subquery_filter = combined_filter_query.subquery()
        filter_statement = sa.and_(
            db.objects.object_type == subquery_filter.c.ObjectID,
            db.objects.image_id == subquery_filter.c.ImageNumber,
            db.objects.object_number == subquery_filter.c.ObjectNumber)
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
                              (db.objects.__tablename__, db.objects.object_id.key),
                              (db.ref_planes.__tablename__, db.ref_planes.channel_name.key),
                              (db.planes.__tablename__, db.stacks.stack_name.key),
                              (db.object_measurements.__tablename__, db.measurement_names.measurement_name.key),
                              (db.object_measurements.__tablename__, db.measurement_types.measurement_type.key)]
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

    def get_hq_filter_triplets(self):
        """
        returns a list of triplets, building the HQ-Filter
        """
        hq = [
               ({
                    db.stacks.stack_name.key: "BinStack",
                    db.ref_planes.channel_name.key: "is-sphere",
                    db.measurement_names.measurement_name.key: "MeanIntensity"
                }, operator.gt, 0)
            ]
        return hq
