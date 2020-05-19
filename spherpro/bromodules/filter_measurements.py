"""
A class to generate filter queries from measurements
and save them into the database.
"""
from itertools import chain, repeat

import numpy as np
import sqlalchemy as sa

import spherpro.bromodules.filter_base as filter_base
import spherpro.db as db


class FilterMeasurements(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.measure_idx = [  # idx_name, default
            (db.objects.object_type.key, 'cell'),
            (db.ref_planes.channel_name.key, None),
            (db.stacks.stack_name.key, 'FullStack'),
            (db.measurement_names.measurement_name.key, 'MeanIntensity'),
            (db.measurement_types.measurement_type.key, None)]
        self.get_filter_vector = get_filter_vector

    def get_measmeta_filter_statements(self, channel_names,
                                       stack_names, measurement_names, measurement_types):
        """
        Generates a filter expression to filter measurements by,
        stack_names, measurement names and measurement types.

        Input:
            channel_names: list of channel names
            stack_names: list ofstack_names
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

        value_lists = [object_types]
        measure_filter = combine_constraints(constraint_columns,
                                             value_lists=value_lists)
        return measure_filter

    def get_filter_data(self, dat_obj, filter_triplets):
        measids = set(f[0] for f in filter_triplets)
        anndat = self.bro.io.objmeasurements.get_measurements(dat_obj, measidx=measids)
        return anndat

    def measmeta_to_measid(self, channel_name=None, stack_name=None, measurement_name=None, measurement_type=None):
        fil = self.get_measmeta_filter_statements(
            channel_names=[channel_name],
            stack_names=[stack_name],
            measurement_names=[measurement_name],
            measurement_types=[measurement_type])
        measid = (self.data.get_measmeta_query()
                  .filter(fil)
                  .with_entities(db.measurements.measurement_id)
                  .all())
        if len(measid) > 1:
            raise ValueError(
                f'Measurment not uniquely specified.\n {len(measid)} measurements found that match specification.')
        return measid[0][0]


def get_filter_vector(anndat, filter_triplets):
    barr = None
    for m, l, t in filter_triplets:
        d = anndat[:, str(m)].X
        tmp = l(d, t)
        if barr is None:
            barr = np.array(tmp)
        else:
            barr = barr & tmp
    return barr


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
                             else c == v
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
            if all(value == sentinel for value in result):
                return
            raise ValueError('sequences of different lengths')
        yield result
