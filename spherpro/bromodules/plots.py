import spherpro.bromodules.plot_scatterplot as plot_scatterplot
import spherpro.bromodules.plot_heatmask as plot_heatmask

class Plots(object):
    def __init__(self, bro):
        self.scatterplot = plot_scatterplot.PlotScatter(bro)
        self.heatmask = plot_heatmask.PlotHeatmask(bro)
