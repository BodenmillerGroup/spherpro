import matplotlib.pyplot as plt
import numpy as np
import pycytools as pct
from matplotlib_scalebar.scalebar import ScaleBar

import spherpro.bromodules.plot_base as plot_base
import spherpro.db as db

"""
TODO:
This is work in progress!
"""


class PlotImage(plot_base.BasePlot):
    """
    A class to plot single images on an axes:
    - intensity image
    - heatmask image
    - contour image

    """

    def __init__(self, bro):
        super().__init__(bro)
        self.io_masks = self.bro.io.masks
        self.filter_measurements = self.bro.filters.measurements
        self.objmeasurements = self.bro.io.objmeasurements
        self.measure_idx = [  # idx_name, default
            (db.ref_planes.channel_name.key, None),
            (db.stacks.stack_name.key, "FullStackFiltered"),
            (db.measurement_names.measurement_name.key, "MeanIntensity"),
            (db.measurement_types.measurement_type.key, None),
        ]


def plot_contour(img_id, object_numbers, values, cmap):
    """
    Plots values
    Args:
        img_id:
        object_numbers:
        values:
        cmap:

    Returns:

    """
    raise NotImplementedError
    pass


def add_scalebar(
    ax, resolution=0.000001, location=4, color="white", pad=0.5, frameon=False, **kwargs
):
    scalebar = ScaleBar(
        resolution, location=location, color=color, pad=pad, frameon=frameon, **kwargs
    )  # 1 pixel = 0.2 meter
    ax.add_artist(scalebar)


def adapt_ax_clims(axs):
    caxs = [ax.images[0] for ax in axs if len(ax.images) > 0]
    clims = [cax.get_clim() for cax in caxs]
    clims = [c for c in clims if c != (True, True)]
    clim_all = [f(c) for f, c in zip([np.min, np.max], zip(*clims))]
    for cax in caxs:
        cax.set_clim(clim_all)


def map_img(mask, values, objectnr):
    timg = pct.library.map_series_on_mask(
        mask, dat_curimg[valuevar], label=dat_curimg[V.COL_OBJ_NR]
    )
    return timg


def add_cb_to_last(axs):
    caxs = [cax for ax in axs for cax in ax.images if ax.images is not None]
    plt.colorbar(caxs[-1], ax=list(axs)[-1])
