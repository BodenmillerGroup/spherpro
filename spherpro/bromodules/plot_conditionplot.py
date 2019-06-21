import spherpro.bromodules.plot_base as plot_base
import spherpro.bromodules.plot_heatmask as plot_heatmask
import spherpro.bromodules.filter_measurements as multifilters
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa
from sqlalchemy.sql.expression import func

import plotnine as gg
import matplotlib.pyplot as plt
from matplotlib_scalebar.scalebar import ScaleBar

LABEL_CBAR = "# of all cells with valid barcodes"
LABEL_Y = "# of cells with\nmost prominent barcode"
LABEL_X = "# of cells with second most prominent barcode"
PLT_TITLE = "Debarcoding Quality"
CBAR_HUE = db.images.bc_valid.key
RADIUS_COL = "radius"


class PlotConditionPlot(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        # make the dependency explicit
        self.heatmask = plot_heatmask.PlotHeatmask(bro)
        self.multifilter = multifilters.FilterMeasurements(bro)


    def plot(self,
        metal = None,
        target = None,
        condition = None,
        title = None,
        censor_min = None,
        censor_max = None,
        relative = None,
        filename = None,
        show = None,
        cm = None,
        filtertuples = None,
        filters = None

    ):
        """
        Plots a plot for a condition and metal, separating the timepoints.

        Args:
            str filename: if specified, saves the plot to the location
            bool show: should plt.show be executed? default False
            condition: the condition tc to be drawn
            cm: color map to use
            filtertuples: list of filtertuples to create subquery from
            filters: list of filternames from db.object_filters
        Returns:
            plt, ax from plot
        """
        if show is None:
            show = False
        if relative is None:
            relative = False
        if censor_min is None:
            censor_min = 0
        if censor_max is None:
            censor_max = 1
        if cm is None:
            cm = plt.cm.winter
        if (metal is None) & (target is None):
            raise NameError('Please specify either a metal or a target!')
        else:
            if metal is None:
                me, dat = self.data.get_metal_from_name(target)
                metal = me[0]
        if condition is None:
            raise NameError('You forgot to specify a condition!')

        if title is None:
            tr, dat = self.data.get_name_from_metal(metal)
            title = condition+"  "+metal+"_"+tr[0]


        if not self.bro.is_debarcoded:
            raise NameError('Please use a debarcoded dataset or debarcode this one!')


        # get the timepoints
        timepoints, wells_per_timepoint = self._get_condition_meta()
        timepoints.sort()
        no = (
            int(np.ceil(wells_per_timepoint/int(np.floor(np.sqrt(wells_per_timepoint))))),
            int(np.floor(np.sqrt(wells_per_timepoint)))
        )
        sizex = len(timepoints) - 1 + len(timepoints) * int(np.floor(np.sqrt(wells_per_timepoint)))
        sizey = int(np.ceil(wells_per_timepoint/int(np.floor(np.sqrt(wells_per_timepoint)))))
        # create array containing the locations of the first condition window
        noy, nox = no
        plot_map = [(i, j) for i in range(0, noy) for j in range(0, nox)]
        fig, ax = self._prepare_grid(sizey, sizex, timepoints, wells_per_timepoint, nox, noy, plot_map, relative)

        # get the Data
        data = self._get_data(metal,filtertuples ,filters)
        # generate lookup table
        lookup = self._generate_wells()
        # create range
        pdat = data[db.object_measurements.value.key]
        clim = (np.percentile(pdat,censor_min*100), np.percentile(pdat,censor_max*100))
        # fill the grid
        fig, ax = self._fill_grid(data, condition, clim, cm, fig, ax, lookup, sizey, sizex, timepoints, wells_per_timepoint, nox, noy, plot_map, relative)
        # some naming stuff and colorbar
        fig.subplots_adjust(right=0.8)
        cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7], frame_on=False, yticks=[], xticks=[])
        cmap = cm
        cax = ax[0,0].get_images()[0]#.imshow(img, cmap=cmap)
        #cax.set_clim(clim[0], clim[1])
        fig.colorbar(cax, ax=cbar_ax)
        plt.suptitle(title)



        if filename is not None:
            fig.savefig(filename)
        else:
            if show:
                fig.show()

        return plt


    def _fill_grid(self, data, condition, clim, cm, fig, ax, lookup, sizey, sizex, timepoints, wells_per_timepoint, nox, noy, plot_map, relative):
        """
        Fills the grid with heatmap plots of the spheres
        """
        for tpit in range(0, len(timepoints)):
            count = 0
            images = list(lookup.loc[(lookup[db.conditions.condition_name.key]==condition)&(lookup[db.conditions.timepoint.key]==timepoints[tpit])][db.images.image_id.key])
            #print(str(tpit)+"---------")
            for well in images:
                #print(well)
                posy, posx = plot_map[count]
                posx = posx + (tpit*(1+nox))
                curax = ax[posy,posx]
                curax.set_facecolor("black")
                # PLOT-----------
                img = self.heatmask.assemble_heatmap_image(data, [int(well)])
                cax = self.heatmask.do_heatplot(img,ax=curax, colorbar=False,crange=clim,update_axrange=False, cmap=cm)
                curax.set_aspect(1)
                curax.tick_params(
                    axis='x',          # changes apply to the x-axis
                    which='both',      # both major and minor ticks are affected
                    bottom='off',      # ticks along the bottom edge are off
                    top='off',         # ticks along the top edge are off
                    labelbottom='off'
                )
                curax.tick_params(
                    axis='y',          # changes apply to the x-axis
                    which='both',      # both major and minor ticks are affected
                    bottom='off',      # ticks along the bottom edge are off
                    top='off',         # ticks along the top edge are off
                    labelbottom='off'
                )
                scalebar = ScaleBar(0.000001, location=4, color='white',pad=0.1, frameon=False, font_properties={'size': 5}) # 1 pixel = 0.2 meter
                curax.add_artist(scalebar)
                curax.set_title(well)
                #fig.colorbar(img, ax=curax)
                # PLOT-----------
                count = count + 1
                if relative:
                    max_h = self.data.main_session.query(func.max(db.masks.shape_h)).first()[0]
                    max_w = self.data.main_session.query(func.max(db.masks.shape_w)).first()[0]
                    cur_h = self.data.main_session.query(db.masks.shape_h).filter(db.masks.image_id == str(well)).first()[0]
                    cur_w = self.data.main_session.query(db.masks.shape_w).filter(db.masks.image_id == str(well)).first()[0]
                    xdiff = max_w - cur_w
                    ydiff = max_h - cur_h
                    curax.set_ylim(bottom=-ydiff/2, top=cur_h+ydiff/2)
                    curax.set_xlim(left=-xdiff/2, right=cur_w+xdiff/2)

        return fig, ax

    def _get_condition_meta(self):
        q = self.session.query(db.conditions.time_point)
        q = q.distinct()
        tpts = pd.read_sql_query(q.statement,self.data.db_conn)
        timepoints = list(tpts[db.conditions.timepoint.key])

        COUNT_COL = "count_1"
        q = self.session.query(sa.func.count(db.conditions.condition_id))\
            .filter(db.conditions.time_point==str(timepoints[0]))
        q = q.group_by(db.conditions.condition_name)
        res = pd.read_sql_query(q.statement,self.data.db_conn)
        wells_per_timepoint = res[COUNT_COL].max()

        return timepoints, wells_per_timepoint

    def _prepare_grid(self, sizey, sizex, timepoints, wells_per_timepoint, nox, noy, plot_map, relative):
        fig, ax = plt.subplots(nrows=sizey, ncols=sizex, subplot_kw={'xticks': [], 'yticks': [], 'aspect':'equal'}, figsize=(50, 6))#,squeeze=True, figsize=(28, 6))

        # paint all the subplots white that will not contain a plot
        for tpit in range(0, len(timepoints)):
            for img in range(wells_per_timepoint, nox*noy):
                posy, posx = plot_map[img]
                posx = posx + (tpit*(1+nox))
                ax[posy,posx].set_facecolor("none")
                ax[posy,posx].set_alpha(0.1)

        # paint the gutters white
        for gutterx in range(nox,(len(timepoints)-1)*(nox+1), nox+1):
            for gutter in ax[range(0,noy),(gutterx)]:
                gutter.set_facecolor("none")
                gutter.set_alpha(0.1)
                gutter.axis('off')


        return fig,ax

    def _generate_wells(self):
        q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=["dist-sphere"],
                             object_ids=['cell'],
                             stack_names=['DistStack'],
                             measurement_names=['MeanIntensity'],
                             measurement_types=['Intensity'],
                         ))
                    )
        data = pd.read_sql_query(q.statement, self.data.db_conn)
        q = self.data.main_session.query(db.conditions.time_point,
                                            db.conditions.condition_name,
                                            db.conditions.bc_plate,
                                            db.conditions.bc_x,
                                            db.conditions.bc_y,
                                            db.images.image_id)
        q = q.join(db.images)
        conditions = pd.read_sql_query(q.statement,self.data.db_conn)
        radius = data[[db.images.image_id.key,db.object_measurements.value.key]].groupby(db.images.image_id.key).max()
        radius.columns = [RADIUS_COL]
        conditions = conditions.set_index(db.images.image_id.key)
        conditions = conditions.join(radius)
        conditions = conditions.sort_values(RADIUS_COL, ascending=False)
        conditions = conditions.drop_duplicates([db.conditions.bc_plate.key,db.conditions.bc_x.key,db.conditions.bc_y.key], keep='first')
        conditions = conditions.reset_index(drop=False)
        return conditions


    def _get_data(self, metal, filtertuples, filters):
        q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=[metal],
                             object_ids=['cell'],
                             stack_names=['FullStack'],
                             measurement_names=['MeanIntensity'],
                             measurement_types=['Intensity'],
                         ))
                    )
        if filtertuples is not None:
            fstatement = self.multifilter.get_multifilter_statement(filtertuples)
            q = q.filter(fstatement)
        if filters is not None:
            for filtername in filters:
                q = q.join(db.object_filters).filter(sa.and_(db.object_filters.FilterName == filtername, db.object_filters.filter_value==1))
        data = pd.read_sql_query(q.statement, self.data.db_conn)
        return data
