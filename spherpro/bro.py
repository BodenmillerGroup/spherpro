import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db

import sqlalchemy as sa

CHANNEL_DISTSPHERE = 'dist-sphere'

def get_bro(fn_config):
    """
    Convenience function to get a bro with a datastore initialized
    with a config file
    Args:
        fn_confio: path to the config file
    Returns:
        A true bro
    """
    store = datastore.DataStore()
    store.read_config(fn_config)
    store.resume_data()
    bro = Bro(store)
    return bro


class Bro(object):
    """docstring for Bro."""

    def __init__(self, DataStore):
        self.data = DataStore
        self.filters = Filters(self)
        self.plots = Plotter(self)

    #########################################################################
    #########################################################################
    #                         preparation functions:                        #
    #########################################################################
    #########################################################################

    def calculate_dist_rim(self, assumed_spheresize = None):
        """calculate_dist_rim
        calculates the distance to rim and saves it to the DB.
        By default, we calculate the distance using the real sphere radius
        saved in the Image meta.
        If you add the assumed_spheresize, then an equal real radius is assumed
        for all spheres.
        The distance_to_rim is calculated using the MeanIntensity of the DistStack.

        Args:
            assumed_spheresize:
                - If None, then take sphere radius from Well images
                - If Float, then assume this for all spheres
                to be the real radius
                - If 'estimate', the real radius is estimated by assuming
                the largest sphere has the correct radius
        """

        if assumed_spheresize is None:
            raise NotImplementedError('This feature is not available yet!')
        elif (type(assumed_spheresize) is float) | (type(assumed_spheresize) is int):
            print("assuming size you gave me!")
        elif (type(assumed_spheresize) is str) & (assumed_spheresize == 'estimate'):
            print("assuming size from all images!")
        else:
            raise NameError('Please specify a valid option!')



    #########################################################################
    #########################################################################
    #                         Quick plot functions:                         #
    #########################################################################
    #########################################################################

    def draw_scatterplot(arg):
        raise NotImplementedError
    
"""
Define Filters
"""
class Filters(object):
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session

    def add_issphere(minfrac=0.01, min_vsother=0.5):
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        outcol_isambiguous = 'is-ambiguous'
        non_zero_offset = 1/100000
        dat_filter = pd.read_sql(
                (session
                     .query(
                                 db.Measurement.ImageNumber,
                                 db.Measurement.ObjectID,
                                 db.Measurement.ObjectNumber,
                                 db.Measurement.Value,
                                 db.PlaneMeta.ChannelName,
                                 db.RefStack.Scale
                                   )
                     .filter(db.Measurement.MeasurementName==col_measure)
                     .filter(db.Measurement.StackName==col_stack)
                    .filter(db.PlaneMeta.ChannelName.in_([col_isother,col_issphere]))
                    .join(db.PlaneMeta)
                      .join(db.RefStack)
                     ).statement,
            store.db_conn)
        dat_filter[db.KEY_VALUE] = (dat_filter[db.KEY_VALUE] * dat_filter[db.KEY_SCALE])
        idx_cols = [c for c in dat_filter.columns
                    if c not in [db.KEY_VALUE, db.KEY_CHANNEL_NAME]]
        dat_filter = dat_filter.pivot_table(values=db.KEY_VALUE,
                               columns=[db.KEY_CHANNEL_NAME])

class Plotter(object):
    DEFAULT_MEASUREMENT_TYPE = 'Intensity'

    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data

    def plt_marker_scatterplot(self, measure_x, measure_y):
        """
        Generates a plot where markers are plotted against each other in a 2D
        scatterplot
        Args:
            measure_x, measure_y: tuples defining the measurements of the form:
                (object_id, channel_name, stack_name, measurement_name, measurement_type)
        Returns:
            p:  the plot figure object

        """
        fil = self._get_measurement_filters(*zip(measure_x, measure_y))
        dat = self._get_measurement_data([fil])
        return dat

    def _get_measurement_filters(self, object_ids, channel_names, stack_names, measurement_names, measurement_types):
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
                              (db.TABLE_MEASUREMENT, db.KEY_OBJECTID),
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
        measure_filter = sa.or_(*constraints)
        return measure_filter

    def _get_measurement_data(self, filters):
        """
        Retrieves filtered measurement data
        """

        query = self._get_measurement_query()
        for fil in filters:
            query = query.filter(fil)
        dat = pd.read_sql(query.statement, self.data.db_conn)
        return dat

    def _get_measurement_query(self):
        query = (self.session.query(db.RefPlaneMeta, db.Measurement)
         .join(db.PlaneMeta)
         .join(db.Measurement)
        )
        return query


