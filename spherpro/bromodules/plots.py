import spherpro.bromodules.plot_scatterplot as plot_scatterplot
import spherpro.bromodules.plot_heatmask as plot_heatmask
import spherpro.bromodules.plot_debarcodequality as plot_debarcodequality

class Plots(object):
    def __init__(self, bro):
        self.scatterplot = plot_scatterplot.PlotScatter(bro)
        self.heatmask = plot_heatmask.PlotHeatmask(bro)
        self.debarcoedequality = plot_debarcodequality.PlotDebarcodeQuality(bro)
