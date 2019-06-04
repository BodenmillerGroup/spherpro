"""
A class to generate filter queries from measurements
and save them into the database.
"""
import re
import operator
import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
from itertools import chain, repeat

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class FilterMeasurements(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.measure_idx =[ # idx_name, default
            (db.objects.object_type.key, 'cell'),
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

    def _get_filter_statement(self, measurement_dict, logical_operator,
                              treshold):
        """
        NEVER USE THIS ALONE BUT JUST THROUGH GET MULTIFILTER STATEMENT
        """
        filter_statement = self.get_measurement_filter_statements(*[[
            measurement_dict.get(o,d)] for o, d in self.measure_idx])
        filter_statement = sa.and_(filter_statement,
                logical_operator(db.object_measurements.value*db.ref_stacks.scale,
                treshold))
        return filter_statement

    def get_multifilter_query(self, query_triplets):
        filters = [self._get_filter_statement(m, l, t)
                   for m, l, t in query_triplets]
        meas_query = (self.data.get_measurement_query()
                .with_entities(db.objects.object_id))
        subquerys = [meas_query.filter(fil).subquery() for fil in
                     filters]
        combined_filter_query = self.session.query(db.objects.object_id)
        for subquery in subquerys:
            combined_filter_query = (combined_filter_query.filter(db.objects.object_id == subquery.c.object_id))

        subquery_filter = combined_filter_query.subquery()
        return subquery_filter

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
                fitlers on the keys: object_id
        """
        subquery = self.get_multifilter_query(query_triplets)
        filter_statement = sa.and_(
            db.objects.object_id == subquery.c.object_id)
        return filter_statement

    def get_measurement_filter_statements(self, object_types, channel_names,
                                          stack_names, measurement_names, measurement_types):
        """
        Generates a filter expression to query for multiple channels, defined as channel_names,
        stack_names, measurement names and measurement types.

        Input:
            object_types: list of object_types
            channel_names: list of channel names
            stack_names: list of stack_names
            measurement_names: list of measurement_names
            measurement_types: list of measurement measurement_types
        Returns:
            A dataframes with the selected measurements
        """
        constraint_columns = [db.objects.object_type,
                              db.ref_planes.channel_name,
                              db.stacks.stack_name,
                              db.measurements.measurement_name,
                             db.measurement_types.measurement_type]

        value_lists = [object_types,
                                        channel_names,
                                        stack_names,
                                        measurement_names,
                                        measurement_types]
        measure_filter = combine_constraints(constraint_columns,
                value_lists=value_lists)
        return measure_filter

    def get_measmeta_filter_statements(self, channel_names,
                                          stack_names, measurement_names, measurement_types):
        """
        Generates a filter expression to filter measurements by,
        stack_names, measurement names and measurement types.

        Input:
            channel_names: list of channel names
            stack_names: list of stack_names
            measurement_names: list of measurement_names
            measurement_types: list of measurement measurement_types
        Returns:
            A dataframes with the selected measurements
        """
        constraint_columns = [
                              db.ref_planes.channel_name,
                              db.stacks.stack_name,
                              db.measurements.measurement_name,
                             db.measurement_types.measurement_type]

        value_lists = [
                                        channel_names,
                                        stack_names,
                                        measurement_names,
                                        measurement_types]
        measure_filter = combine_constraints(constraint_columns,
                value_lists=value_lists)
        return measure_filter


    def get_objectmeta_filter_statements(self, object_types):
        """
        Generates a filter expression to filter measurements by,
        stack_names, measurement names and measurement types.

        Input:
            channel_names: list of channel names
            stack_names: list of stack_names
            measurement_names: list of measurement_names
            measurement_types: list of measurement measurement_types
        Returns:
            A dataframes with the selected measurements
        """
        constraint_columns = [db.objects.object_type]

        value_lists = [object_types
                                        ]
        measure_filter = combine_constraints(constraint_columns,
                value_lists=value_lists)
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

def combine_constraints(columns, value_lists):
    """
    Combine constraints on columns in an sqlalchemy query.
    Args:
        columns: table columns to constrain on
        value_lists: lists of constraints:
            either contain elements
            OR tuples with multiple elements
            OR None to not constrain
    Returns:
        A filter statement encoding the constraints.
    """
    constraints = [sa.and_(*[c.in_(v) if isinstance(v, tuple)
        else c==v
        for c, v in zip_exact(columns, values) if v is not None])
        for values in zip_exact(*value_lists)]
    if len(constraints) > 1:
        measure_filter = sa.or_(*constraints)
    else:
        measure_filter = constraints[0]
    return measure_filter

def zip_exact(*args):
    """
    A zip that asserts that all list have equal length
    """
    sentinel = object()
    iters = [chain(it, repeat(sentinel)) for it in args]
    for result in zip(*iters):
        if sentinel in result:
            if all(value==sentinel for value in result):
                return
            raise ValueError('sequences of different lengths')
        yield result
