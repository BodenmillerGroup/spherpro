import spherpro.bromodules.plot_base as plot_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import plotnine as gg
import matplotlib.pyplot as plt

LABEL_CBAR = "# of all cells with valid barcodes"
LABEL_Y = "# of cells with\nmost prominent barcode"
LABEL_X = "# of cells with second most prominent barcode"
PLT_TITLE = "Debarcoding Quality"
CBAR_HUE = db.KEY_BCVALID


class PlotConditionPlot(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        # make the dependency explicit


    def plot(self,
        filename = None,
        show = None,
        cm = None
    ):
        """
        Plots a plot for a condition and metal, separating the timepoints.

        Args:
            str filename: if specified, saves the plot to the location
            bool show: should plt.show be executed? default False
            cm: color map to use
        Returns:
            plt, ax from plot
        """
        if show is None:
            show = False
        if not self.bro.is_debarcoded:
            raise NameError('Please use a debarcoded dataset or debarcode this one!')


        # get the timepoints
        timepoints = self._get_timepoints()



        if filename is not None:
            plt.savefig(filename)
        else:
            if show:
                plt.show()

        return plt, ax


    def mname(self):
        q = self.session.query(db.Condition.TimePoint)
        q = q.distinct()
        tpts = pd.read_sql_query(q.statement,self.data.db_conn)
        return list(tpts[db.KEY_TIMEPOINT])
