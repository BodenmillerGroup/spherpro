import spherpro.bromodules.plot_base as plot_base

import copy
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db

import pycytools as pct
import pycytools.library

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

class InteractiveHeatplot(object):
    def __init__(self, session, plotter):
        self.session = session
        self.plotter = plotter

    def selector_basic(self, ax=None):
        if ax is None:
            fig, ax = plt.subplots(1)
        name_dict = {m: n for m, n in self.session.query(db.pannel.metal,
                                                  db.pannel.target).all()}
        # make the sites widget
        sites = [ s[0] for s in self.session.query(db.sites.site_id).all()]
        # add the imageidx widget and make it adapt to the
        channel_names = [q[0] for q in
                         self.session.query(db.ref_planes.channel_name).all()]
        stack_names = [q[0] for q in
                       self.session.query(db.stacks.stack_name).all()]
        object_names = [q[0] for q in self.session.query(db.masks.object_type).distinct()]
        measurement_names = [q[0] for q in
                             self.session.query(db.measurement_names.measurement_name).all()]
        channel_names_info = [pct.library.name_from_metal(c, name_dict) for c in channel_names]

        ipw.interact(self._selector_basic_plot,
            site=sites,
            roi_idx=ipw.IntSlider(min=0, max=30, continuous_update=False),
            img_idx=ipw.IntSlider(min=0, max=70, continuous_update=False),
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

    def _selector_basic_plot(self, site, roi_idx, img_idx, stat, stack, channel, transform, censor_min,
                     censor_max, keepRange, filter_hq, ax):
        q = (self.session.query(db.images.image_id)
                .join(db.valid_images)
                .join(db.acquisitions)
                .filter(db.acquisitions.site_id == site))
        if roi_idx > 0:
            r_id = (self.session.query(db.acquisitions.acquisition_id)
                    .filter(db.acquisitions.site_id == site)
                    .order_by(db.acquisitions.acquisition_id)
                    .offset(roi_idx-1)).first()
            if r_id is None:
                return
            q = q.filter(db.acquisitions.acquisition_id == r_id)

        if img_idx == 0:
            imnr = [r[0] for r in q.distinct()]
        else:
            imnr = [q.order_by(db.images.image_id).offset(img_idx-1).first()[0]]
            print(imnr)

        if imnr[0] is None:
            return
        metal = pct.library.metal_from_name(channel)
        self.plotter.plt_heatplot(imnr, stat, stack, metal, transform=transform, censor_min=censor_min,
                     censor_max=censor_max, keepRange=keepRange, ax=ax, title=channel)

    def get_dynamic_selector(self, plotfkt):
        ALL = 'all'
        name_dict = {m: n for m, n in self.session.query(db.pannel.metal,
                                                  db.pannel.target).all()}
        # make the sites widget
        sites = [ s[0] for s in self.session.query(db.sites.site_id).all()]
        w_sites = ipw.Select(sites)
        # add the roi widget and make it adapt depending on the chosen site
        w_roi = ipw.Select([ALL] + self.get_roi_ids(w_sites.value))
        def update_roi_range(*args):
            w_roi.options = [ALL] + self.get_roi_ids(w_sites.value)
        w_sites.observe(update_roi_range, 'value')
        # add the imageidx widget make it adapt depending on the chosen site
        w_image = ipw.Select([ALL] + self.get_image_ids(w_sites.value))
        def update_img_range(*args):
            w_image.options = [ALL] + self.get_img_ids(w_roi.value)
        w_roi.observe(update_img_range, 'value')
        w_imgidx = ipw.__version__
        channel_names = [q[0] for q in
                         self.data.main_session.query(db.ref_planes.channel_name).all()]
        stack_names = [q[0] for q in
                       self.data.main_session.query(db.stacks.stack_name).all()]
        object_names = [q[0] for q in self.data.main_session.query(db.masks.object_type).distinct()]
        measurement_names = [q[0] for q in
                             self.data.main_session.query(db.measurement_names.measurement_name).all()]
        channel_names_info = [pct.library.name_from_metal(c, name_dict) for c in channel_names]

        ipw.interact(self.plt_heatplot,
            site=sites,
            roi_idx=ipw.IntSlider(min=0, max=30, continuous_update=False),
            img_idx=ipw.IntSlider(min=0, max=30, continuous_update=False),
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

    def get_roi_ids(self, site_id):
        q_rois = (self.session.query(db.acquisitions.acquisition_id)
                    .filter(db.acquisitions.site_id == site_id)
                    .order_by(db.acquisitions.acquisition_id)
                    .all())
        rois = [r[0] for r in q_rois]
        return rois

    def get_img_ids(self, site_id):
        q_rois = (self.session.query(db.acquisitions.acquisition_id)
                    .filter(db.acquisitions.site_id == site_id)
                    .order_by(db.acquisitions.acquisition_id)
                    .all())
        rois = [r[0] for r in q_rois]
        return rois


class PlotHeatmask(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        self.io_masks = self.bro.io.masks
        self.filter_measurements = self.bro.filters.measurements
        self.objmeasurements = self.bro.io.objmeasurements
        self.measure_idx =[ # idx_name, default
            (db.ref_planes.channel_name.key, None),
            (db.stacks.stack_name.key, 'FullStack'),
            (db.measurement_names.measurement_name.key, 'MeanIntensity'),
            (db.measurement_types.measurement_type.key, None)]
        self.interactive = InteractiveHeatplot(self.data.main_session, self)


    def _prepare_masks(self, image_numbers):
        masks = [self.io_masks.get_mask(i) for i in image_numbers]
        return masks

    def _prepare_slices(self, image_numbers):
        dat = (self.session.query(db.images.image_id, db.images.image_pos_x,
                                  db.images.image_pos_y, db.images.image_shape_w,
                                  db.images.image_shape_h)
                    .filter(db.images.image_id.in_(image_numbers))).all()
        slice_dict = {i: (np.s_[x:(x+w)], np.s_[y:(y + h)])
                          for i, x, y, h, w in dat}
        slices = [slice_dict[i] for i in image_numbers]
        return slices

    def get_heatmask_data(self, measurement_dict, image_numbers=None, filters=None, valid_objects=True,
                          valid_images=True, object_type='cell'):

        if filters is None:
            filters = []

        filter_statement = self.filter_measurements.get_measmeta_filter_statements(*[[
            measurement_dict.get(o,d)] for o, d in self.measure_idx ])

        q_meas = (self.data.get_measmeta_query(session=self.session)
                  .filter(filter_statement)
                  .add_column(db.ref_stacks.scale)
                  )
        q_obj = (self.data.get_objectmeta_query(session=self.session,
                                                valid_objects=valid_objects,
                                                valid_images=valid_images)
                    .filter(db.objects.object_type == object_type)
                 )

        # add more output columns
        q_obj = q_obj.add_columns(db.objects.object_number)

        if image_numbers is not None:
            q_obj = q_obj.filter(db.images.image_id.in_(image_numbers))
        if len(filters) > 0:
            q_obj = q_obj.join(db.object_filters)
        for fil in filters:
            # TODO: this NEEDs to be fixed as it wont work as expected with multiple filters!
            # This needs to be done with subqueries!
            q_obj = q_obj.filter(fil)

        data = self.objmeasurements.get_measurements(q_obj=q_obj, q_meas=q_meas)
        data = self.objmeasurements.scale_anndata(data)
        data = self.objmeasurements.convert_anndata_legacy(data)
        return data

    def assemble_heatmap_image(self, dat_cells, image_numbers=None, cut_slices=None,
                               cut_masks=None, out_shape=None, value_var=None):
        """
        TODO: rewrite and move to pycytools
        Assembles single cell data, masks and slices with the mask positions
        into one heatmap image
        """
        cut_id_name = db.images.image_id.key
        cell_id_name = db.objects.object_number.key

        if value_var is None:
            value_var = db.object_measurements.value.key

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

    @staticmethod
    def do_heatplot(img, title=None, crange=None, ax=None, update_axrange=True, cmap=None, colorbar =True,
                    cmap_mask=None, cmap_mask_alpha=0.3, bad_color='k', bad_alpha=1):

        #if crange is None:
        #    crange=(np.nanmin(img[:]), np.nanmax(img[:]))

        if cmap is None:
            cmap = copy.copy(plt.cm.viridis)
        cmap.set_bad(bad_color, bad_alpha)
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

        cax.set_data(np.flipud(img))
        cax.set_extent([0,img.shape[1], 0, img.shape[0]])
        if crange is not None:
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
            if np.any(mask_img):
                mask_img = np.ma.array(mask_img, mask=img.mask | (mask_img == False))
                if cmap_mask is None:
                    cmap_mask='Greys'
                ax.imshow(mask_img == 1, cmap=cmap_mask, alpha=cmap_mask_alpha)
        return ax

    def plt_heatplot(self, img_ids, stat, stack, channel, transform=None, censor_min=0,
                     censor_max=1, keepRange=False, filters=None, filter_hq=None,
                     ax=None, title=None, colorbar=True, transform_fkt=None, cmap=None):
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

        #if filter_hq:
        #     fil = [sa.and_(db.object_filters.=='is-hq', db.object_filters.filter_value==True)]
        #else:
        #     fil = None
        if transform is None:
            transform = 'none'

        fil=filters
        #print('Start loading...')
        data = self.get_heatmask_data({db.ref_planes.channel_name.key: channel,
                                                db.stacks.stack_name.key: stack,
                                                db.measurement_names.measurement_name.key: stat},
                                                image_numbers=img_ids,
                                                filters=fil
                                               )
        #print(data.shape)
        #print('Finished loading!')
        col_val = db.object_measurements.value.key
        if transform_fkt is None:
            transform_fkt = transf_dict[transform]
        data[col_val] = transform_fkt(data[col_val])
        if (data.shape[0] == 0):
            if ax is None:
                fig = plt.figure()
                a = fig.axes[0]
            else:
                a = ax
        else:
            img = self.assemble_heatmap_image(data)
            if (censor_min > 0) | (censor_max < 1):
                crange = (np.percentile(data[col_val], censor_min*100),
                        np.percentile(data[col_val], censor_max*100))
            else:
                crange = (data[col_val].min(), data[col_val].max())

            if title is None:
                title=channel

            a = self.do_heatplot(img,  title=title,
                    crange=crange, ax=ax, update_axrange=~keepRange, colorbar=colorbar, cmap=cmap)
        a.axis('off')
