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
CBAR_HUE = db.images.bc_valid.key


class PlotDebarcodeQuality(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        # make the dependency explicit


    def quality_plot(self,
        filename = None,
        show = None,
        cm = None
    ):
        """
        Plots a quality Plot for the debarcoding. This function requires
        a debarcoded dataset!

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

        # get data
        table, zeros = self._get_data()
        # plot data
        plt, ax = self._produce_plot(table, zeros, cm = cm)

        if filename is not None:
            plt.savefig(filename)
        else:
            if show:
                plt.show()

        return plt, ax

    def _get_data(self):
        q = self.data.main_session.query(db.images)
        zeros = q.filter(db.images.condition_id == None).count()
        q = q.filter(db.images.condition_id.isnot(None)).statement
        table = pd.read_sql_query(q, self.data.db_conn)
        return table, zeros

    def _produce_plot(self, data, zeros,
        cm = None
    ):
        fig, ax = plt.subplots()

        if cm is None:
            cm = plt.cm.get_cmap('winter')

        y = data[db.images.bc_highest_count.key]
        x = data[db.images.bc_second_count.key]
        sc = ax.scatter(x, y,
                   alpha=0.7, edgecolors='none', c=data[CBAR_HUE], cmap=cm)
        upper = max(x.max()*1.1, y.max()*1.1)
        ax.set_xlim([0, upper])
        ax.set_ylim([0, upper])
        #ax.legend()
        ax.grid(True)
        ax.set_aspect(1)
        plt.plot([0, upper], [0, upper], 'k-', c="grey", lw=1, alpha=0.5, label="_not in legend")
        cbar = plt.colorbar(sc)
        cbar.set_label(LABEL_CBAR)

        plt.ylabel(LABEL_Y)
        plt.xlabel(LABEL_X)

        plt.title(PLT_TITLE)
        return plt, ax
