import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import plotnine as gg

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
        
    def add_issphere(self, minfrac=0.01):
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        non_zero_offset = 1/100000
        dat_filter = pd.read_sql(
                (self.session
                     .query(
                                 db.Measurement.ImageNumber,
                                 db.Measurement.ObjectID,
                                 db.Measurement.ObjectNumber,
                                 db.Measurement.Value,
                                 db.RefPlaneMeta,
                                 db.RefStack.Scale
                                   )
                    .filter(db.Measurement.MeasurementName==col_measure)
                    .filter(db.Measurement.StackName==col_stack)
                    .filter(db.RefPlaneMeta.ChannelName.in_([col_isother,col_issphere]))
                    .join(db.PlaneMeta)
                    .join(db.RefPlaneMeta)
                    .join(db.RefStack)
                     ).statement,
            self._conn)
        dat_filter[db.KEY_VALUE] = (dat_filter[db.KEY_VALUE] * dat_filter[db.KEY_SCALE])
        idx_cols = [c for c in dat_filter.columns
                    if c not in [db.KEY_VALUE, db.KEY_CHANNEL_NAME,
                                 db.KEY_PLANEID]]
        dat_filter = dat_filter.pivot_table(values=db.KEY_VALUE,
                               columns=[db.KEY_CHANNEL_NAME], index=idx_cols)
        dat_filter =  pd.DataFrame.from_dict({outcol_issphere: (
            (dat_filter[col_issphere]+non_zero_offset)/(
                dat_filter[col_isother]+non_zero_offset) > minfrac)},
            orient='columns')
        dat_filter.columns.names = [db.KEY_FILTERNAME]
        dat_filter = dat_filter.stack()
        dat_filter.name = db.KEY_FILTERVALUE
        dat_filter = dat_filter.reset_index(drop=False)
        dat_filter = dat_filter.loc[:,
                       self.bro.data._get_table_columnnames(db.TABLE_FILTERS)]
        query = self.session.query(db.Filters).filter(db.Filters.FilterName ==
                                                      outcol_issphere)
        table = self.bro.data._get_table_object(db.TABLE_FILTERS)
        self.bro.data._add_generic_tuple(dat_filter,
        query=query, table=table, replace=True)
        return dat_filter

    @property
    def _conn(self):
        return self.session.connection()

class Plotter(object):

    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data
        # define the measurement indexes with defaults
        # order is order expected by _get_measurement_filters
        self.measure_idx =[ # idx_name, default
            (db.KEY_OBJECTID, 'cell'),
            (db.KEY_CHANNEL_NAME, None),
            (db.KEY_STACKNAME, 'FullStack'),
            (db.KEY_MEASUREMENTNAME, 'MeanIntensity'),
            (db.KEY_MEASUREMENTTYPE, 'Intensity')]


    def plt_marker_scatterplot(self, measure_x, measure_y, image_ids=None, filter_name=None):
        """
        Generates a plot where markers are plotted against each other in a 2D
        scatterplot
        Args:
            measure_x, measure_y: dict defining the selected measures:
                {db.KEY_OBJECTID: object_id,
                 db.KEY_CHANNEL_NAME: channel_name,
                 db.KEY_STACKNAME: stack_name,
                 db.KEY_MEASUREMENTNAME: measurement_name,
                 db.KEY_MEASUREMENTTYPE: measurement_type}
        Returns:
            p:  the plot figure object

        """
        dat = self.get_marker_scatterplot_data([measure_x, measure_y],
                                               image_ids=image_ids,
                                               filter_name=filter_name)
        p = (gg.ggplot(dat, gg.aes(x='0_Value', y='1_Value')) +
          gg.geom_bin2d()+
          gg.geom_smooth(method='lm') +
          gg.xlab(' - '.join([measure_x.get(o,d) for o, d in
                              self.measure_idx])) +
          gg.ylab(' - '.join([measure_y.get(o,d) for o, d in
                              self.measure_idx])) +
          gg.scale_x_sqrt()+
          gg.scale_y_sqrt()
        )
        return p


    def get_marker_scatterplot_data(self, measures, image_ids=None,
                                    filter_name=None):


        filters = [self._get_measurement_filters(*[[meas.get(o,d)] for o, d in
                                                   self.measure_idx ])
                   for meas in measures]
        query = self._get_measurement_query()
        if image_ids is not None:
            query = query.filter(db.Measurement.ImageNumber.in_(image_ids))
        if filter_name is not None:
            query = (query.filter(sa.and_(db.Filters.FilterName == filter_name,
                                         db.Filters.FilterValue == True))
                     .join(db.Filters))

        query_joins = self._get_joined_filtered_queries(query, filters)
        dat = pd.read_sql(query_joins.statement, self.data.db_conn)
        return dat

    def _get_joined_filtered_queries(self, base_query, filters, on_cols=None):
        """
        Queries repeatedly using the base_query and applying multiple filters.
        The results will be joined on the 'on_cols' and the columns renamed
        with a prefix according to the filter index (0_, 1_ etc.)
        
        Args:
            base_query: the query that should be used as a basis.
            filters:    a list of filter statements
            on_cols:    a list of column names. Default: ['ImageNumber',
            'ObjectNumber']
        Returns:
            the query to get the results
        """
        if on_cols is None:
            on_cols = [db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER]
        queries = [base_query.filter(fil).subquery(name=str(i))
                   for i, fil in enumerate(filters)]
        # queries =[q.apply_labels() for q in queries]
        query_joins = self.session.query(*queries)
        for i in range(len(queries)-1):
            query_joins = query_joins.join(
                queries[i+1],
                (sa.and_(*[
                    getattr(queries[0].c, col) ==
                    getattr(queries[i+1].c, col) for col in
                           on_cols])))
        query_joins = query_joins.with_labels()
        return query_joins


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
        query = (self.session.query(db.RefPlaneMeta.ChannelName,
                                    db.RefPlaneMeta.ChannelType,
                                    db.Image.ImageNumber,
                                   db.Objects.ObjectNumber,
                                   db.Objects.ObjectID,
                                   db.Measurement.MeasurementName,
                                   db.Measurement.MeasurementType,
                                   db.Measurement.Value,
                                   db.Measurement.PlaneID)
         .join(db.PlaneMeta)
         .join(db.Measurement)
        .join(db.Objects)
        .join(db.Image)
                )
        return query
