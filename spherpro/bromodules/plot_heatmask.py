import spherpro.bromodules.plot_base as plot_base


import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db

import pycytools as pct

import sqlalchemy as sa
import matplotlib.pyplot as plt

import ipywidgets as ipw
import functools


# TODO: move to the pycytools!
def logtransf_data(x):
    xmin = min(x[x>0])
    return(np.log(x+xmin))


def asinhtransf_data(x):
    return np.arcsinh(x/5)

transf_dict = {'none': lambda x: x,
                                           'log': logtransf_data,
                                         'asinh': asinhtransf_data,
                                         'sqrt': np.sqrt}

class PlotHeatmask(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        self.io_masks = self.bro.io.masks
        self.filter_measurements = self.bro.filters.measurements
        self.measure_idx =[ # idx_name, default
            (db.KEY_OBJECTID, 'cell'),
            (db.KEY_CHANNEL_NAME, None),
            (db.KEY_STACKNAME, 'FullStack'),
            (db.KEY_MEASUREMENTNAME, 'MeanIntensity'),
            (db.KEY_MEASUREMENTTYPE, 'Intensity')]


    def _prepare_masks(self, image_numbers):
        masks = [self.io_masks.get_mask(i) for i in image_numbers]
        return masks

    def _prepare_slices(self, image_numbers):
        dat = (self.session.query(db.Masks.ImageNumber, db.Masks.PosX,
                                  db.Masks.PosY, db.Masks.ShapeW,
                                  db.Masks.ShapeH)
                    .filter(db.Masks.ImageNumber.in_(image_numbers))).all()
        slice_dict = {i: (np.s_[x:(x+w)], np.s_[y:(y + h)])
                          for i, x, y, w, h in dat}
        slices = [slice_dict[i] for i in image_numbers]
        return slices
    
    def get_heatmask_data(self, measurement_dict, image_numbers=None, filters=None):

        if filters is None:
            filters = []

        filter_statement = self.filter_measurements.get_measurement_filter_statements(*[[
            measurement_dict.get(o,d)] for o, d in self.measure_idx ])

        query = self.data.get_measurement_query(session=self.session)

        query = query.filter(filter_statement)

        if image_numbers is not None:
            query = query.filter(db.Image.ImageNumber.in_(image_numbers))
        if len(filters) > 0:
            query = query.join(db.Filters)
        for fil in filters:
            # TODO: this NEEDs to be fixed as it wont work as expected with multiple filters!
            # This needs to be done with subqueries!
            query = query.filter(fil)

        data = pd.read_sql(query.statement, self.data.db_conn)
        return data

    def assemble_heatmap_image(self, dat_cells, image_numbers=None, cut_slices=None,
                               cut_masks=None, out_shape=None, value_var=None):
        """
        Assembles single cell data, masks and slices with the mask positions
        into one heatmap image
        """
        cut_id_name = db.KEY_IMAGENUMBER
        cell_id_name = db.KEY_OBJECTNUMBER

        if value_var is None:
            value_var = db.KEY_VALUE

        if image_numbers is None:
            image_numbers = dat_cells[cut_id_name].unique().tolist()

        if cut_slices is None:
            cut_slices = self._prepare_slices(image_numbers)

        if cut_masks is None:
            cut_masks = self._prepare_masks(image_numbers)

        if out_shape is None:
            x_start, x_stop, y_start, y_stop = \
                    zip(*[(slx.start, slx.stop, sly.start, sly.stop)
                          for slx, sly in cut_slices])
            new_shape = (max(x_stop), max(y_stop))
        else:
            new_shape = out_shape

        pimg = np.empty(new_shape)
        pimg[:] = np.NAN
        notbg = np.zeros(new_shape)

        dat_cells = dat_cells.set_index(cut_id_name)

        for cid, sl, mask in zip(image_numbers, cut_slices, cut_masks):
            if cid in dat_cells.index:
                intensity = dat_cells.loc[cid,:]
                intensity = intensity.set_index(cell_id_name)
                intensity = intensity[value_var]
                timg= pct.library.map_series_on_mask(
                    mask, intensity,
                    label=intensity.index)
                pimg_sl =pimg[sl]

                fil = (timg.mask == False) & (np.isnan(timg.imag) == False)  &(
                    np.isnan(pimg_sl) == True)
                pimg_sl[fil] = timg[fil]
                fil2 = notbg[sl] == 0
                notbg[sl][fil2] = (timg.mask==False)[fil2]

        pimg = np.ma.array(pimg, mask=(notbg ==0))

        if out_shape is None:
            pimg = pimg[min(x_start):, min(y_start):]

        return(pimg)

    def do_heatplot(self, img, title=None,crange=None, ax=None, update_axrange=True, cmap=None, colorbar =True):

        if crange is None:
            crange=(np.nanmin(img[:]), np.nanmax(img[:]))

        if cmap is None:
            cmap = plt.cm.viridis
        cmap.set_bad('k',1.)
        if ax is None:
            plt.close()
            fig, ax = plt.subplots(1, 1)
        else:
            fig = ax.get_figure()

        if len(ax.images) == 0:
            cax = ax.imshow(img, cmap=cmap)
            if colorbar:
                fig.colorbar(cax)
        else:
            cax = ax.images[0]
            if len(ax.images) == 2:
                cax_mask = ax.images[1]
                cax_mask.remove()



        cax.set_data(np.flipud((img)))
        cax.set_extent([0,img.shape[1], 0, img.shape[0]])
        cax.set_clim(crange[0], crange[1])

        # update bounts

        if update_axrange == True:
            ax.set_xbound(lower=0, upper=img.shape[1])
            ax.set_ybound(lower=0, upper=img.shape[0])
        else:
            bndx = ax.get_xbound()
            ax.set_xbound(upper=min(img.shape[1], bndx[1]))
            bndy = ax.get_ybound()
            ax.set_ybound(upper=min(img.shape[0], bndy[1]))

        if title is not None:
            ax.set_title(title)
        #fig.colorbar(cax)
        # slightly color the non background but not colored cells

        if hasattr(img, 'mask'):
            mask_img = np.isnan(img)
            mask_img = np.ma.array(mask_img, mask=img.mask | (mask_img == False))
            ax.imshow(mask_img, alpha=0.2)

        #fig.canvas.draw()
        return ax

    def plt_heatplot(self, site, img_idx, stat, stack, channel, transform, censor_min,
                     censor_max, keepRange, filter_hq, ax=None):
        """
        Retrieves images form the database and maps then on masks
        Args:
            site: sitename
            img_idx: 0: all images from the site are ploted
                     #: the #th image form this site
            stat: The KEY_MEASUREMENTNAME 
            stack: the Stackname
            channel: the ChannelName
            transform: a transform

        """
        if ax is None:
            ax = self._ax

        image_numbers = [q[0] for q in (self.data.main_session.query(db.Image.ImageNumber)
                                         .filter(db.Image.SiteName == site).distinct())]
        if filter_hq:
             fil = [sa.and_(db.Filters.FilterName=='is-hq', db.Filters.FilterValue==True)]
        else:
             fil = None
        if img_idx == 0:
            imnr = image_numbers
        else:
            imnr = [image_numbers[img_idx]]
        metal = pct.library.metal_from_name(channel)
        print('Start loading...')
        data = self.get_heatmask_data({db.KEY_OBJECTID: 'cell',
                                                db.KEY_CHANNEL_NAME: metal,
                                                db.KEY_STACKNAME: stack,
                                                db.KEY_MEASUREMENTNAME: stat},
                                                image_numbers=imnr,
                                                filters=fil
                                               )
        print('Finished loading!')
        data['Value'] = transf_dict[transform](data['Value'])
        img = self.assemble_heatmap_image(data)
        crange = ( np.percentile(data['Value'],censor_min*100),
                  np.percentile(data['Value'],censor_max*100))

        self.do_heatplot(img,  title=channel,
                   crange=crange, ax=ax, update_axrange=keepRange==False)
        plt.axis('off')
        
    def ipw_heatplot(self, ax):

        sites = [ s[0] for s in self.data.main_session.query(db.Image.SiteName).distinct()]
        name_dict = {m: n for m, n in self.data.main_session.query(db.Pannel.Metal,
                                                                  db.Pannel.Target)}
        channel_names = [q[0] for q in
                         self.data.main_session.query(db.RefPlaneMeta.ChannelName).distinct()]
        stack_names = [q[0] for q in
                       self.data.main_session.query(db.Measurement.StackName).distinct()]
        object_names = [q[0] for q in self.data.main_session.query(db.Objects.ObjectID).distinct()]
        measurement_names = [q[0] for q in
                             self.data.main_session.query(db.MeasurementName.MeasurementName).distinct()]
        channel_names_info = [pct.library.name_from_metal(c, name_dict) for c in channel_names]

        ipw.interact(self.plt_heatplot,
            site=sites,
            img_idx=ipw.IntSlider(min=0, max=137, continuous_update=False),
            stat=measurement_names,
            stack=stack_names,
            channel=channel_names_info,
            transform=list(transf_dict.keys()),
            censor_min=ipw.FloatSlider(min=0, max=0.5, value=0, step=0.0001, continuous_update=False),
            censor_max=ipw.FloatSlider(min=0.5, max=1, value=1, step=0.0001, continuous_update=False),
            keepRange=ipw.Checkbox(),
            filter_hq=ipw.Checkbox(value=True),
            ax=ipw.fixed(ax)
        )

