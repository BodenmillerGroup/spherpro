import spherpro.bromodules.plot_base as plot_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import plotnine as gg

VAL_COL = db.object_measurements.value.key

class PlotScatter(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        # make the dependency explicit

        self.filter_measurements = self.bro.filters.measurements
        # define the measurement indexes with defaults
        # order is order expected by _get_measurement_filters
        self.measure_idx =[ # idx_name, default
            (db.objects.object_id.key, 'cell'),
            (db.ref_planes.channel_name.key, None),
            (db.stacks.stack_name.key, 'FullStack'),
            (db.measurement_names.measurement_name.key, 'MeanIntensity'),
            (db.measurement_types.measurement_type.key, 'Intensity')]


    def plot_bin2d(self, measure_x, measure_y, image_ids=None, filters=None):
        """
        Generates a plot where markers are plotted against each other in a 2D
        scatterplot
        Args:
            measure_x, measure_y: dict defining the selected measures:
                {db.objects.object_id.key: object_id,
                 db.ref_planes.channel_name.key: channel_name,
                 db.stacks.stack_name.key: stack_name,
                 db.measurement_names.measurement_name.key: measurement_name,
                 db.measurement_types.measurement_type.key: measurement_type}
        Returns:
            p:  the plot figure object

        """
        dat = self.get_marker_data([measure_x, measure_y],
                                               image_ids=image_ids,
                                               filters=filters)
        p = (gg.ggplot(dat, gg.aes(x="0_"+VAL_COL, y='1_'+VAL_COL)) +
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


    def plot_points(self, measure_x, measure_y, image_ids=None, filters=None):
        """
        Generates a plot where markers are plotted against each other in a 2D
        scatterplot
        Args:
            measure_x, measure_y: dict defining the selected measures:
                {db.objects.object_id.key: object_id,
                 db.ref_planes.channel_name.key: channel_name,
                 db.stacks.stack_name.key: stack_name,
                 db.measurement_names.measurement_name.key: measurement_name,
                 db.measurement_types.measurement_type.key: measurement_type}
            image_ids: image ids to plot
            filters: list of filter tuples [('filtername1', True),
            ('filtername2', False)]
        Returns:
            p:  the plot figure object

        """
        dat = self.get_marker_data([measure_x, measure_y],
                                               image_ids=image_ids,
                                               filters=filters)
        p = (gg.ggplot(dat, gg.aes(x='0_'+VAL_COL, y='1_'+VAL_COL)) +
          gg.geom_point()+
          gg.geom_smooth(method='lm') +
          gg.xlab(' - '.join([measure_x.get(o,d) for o, d in
                              self.measure_idx])) +
          gg.ylab(' - '.join([measure_y.get(o,d) for o, d in
                              self.measure_idx])) +
          gg.scale_x_sqrt()+
          gg.scale_y_sqrt()
        )
        return p

    def get_marker_data(self, measures, image_ids=None,
                                    filters=None):


        filters_measurement = [self.filter_measurements.get_measurement_filter_statements(*[[meas.get(o,d)] for o, d in
                                                   self.measure_idx ])
                   for meas in measures]
        query = self._get_measurement_query()
        if image_ids is not None:
            query = query.filter(db.images.image_id.in_(image_ids))
        query_joins = self._get_joined_filtered_queries(query,
                                                        filters_measurement)
        if filters is not None:
           # TODO: This needs to be fixed!
           query = query.filter(filters)
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
            on_cols = [db.images.image_id.key, db.objects.object_number.key]
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
        query = self.data.get_measurement_query(session=self.session)
        return query
