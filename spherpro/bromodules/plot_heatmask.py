import spherpro.bromodules.plot_base as plot_base


import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db

import pycytools as pct

import sqlalchemy as sa

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
        filters.append(filter_statement)
        
        query = self.data.get_measurement_query(session=self.session)
        query = query.join(db.Filters)

        if image_numbers is not None:
            query = query.filter(db.Image.ImageNumber.in_(image_numbers))

        for fil in filters:
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

    def do_heatplot(img, title=None,crange=None, ax=None, update_axrange=True, cmap=None, colorbar =True):
        
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
        


    def plt_heatplot(site_id, cut_id,stat, channel, transform, censor_min, censor_max, keepRange, filter_hq,ax=None):
        
        if ax is None:
            ax = plt.gca()
        channel = pct.library.metal_from_name(channel)
        image_ids = dat_image.xs(site_id, level='site')['ImageNumber']
        tdat = plot_cells.loc[list(image_ids),(stat,channel)]
        if filter_hq:
            tdat = tdat.loc[plot_cells['filter']['is-hq'], ]

        if transform is not 'none':
            tdat = transf_dict[transform](tdat)
            
        tdat = tdat.dropna()
        tdat.loc[~np.isfinite(tdat)] = np.min(tdat.loc[np.isfinite(tdat)])
        
        crange = ( np.percentile(tdat,censor_min*100), np.percentile(tdat,censor_max*100))
        print(crange)
        if cut_id >0:
            image_id = image_ids[cut_id-1]
            real_cutid = image_ids.index[cut_id-1]
            tdat = tdat.loc[image_id]
            print(real_cutid)
            img = pct.library.map_series_on_mask(dat_image.loc[(site_id, real_cutid), 'mask'], tdat)
        else:
            img = assemble_heatmap_image(dat_image.xs(site_id, level='site')['ImageNumber'],
                                         dat_image.xs(site_id, level='site')['slice'],
                                         dat_image.xs(site_id, level='site')['mask'],
                                      tdat, out_shape = None)
            
        do_heatplot(img,
                    title=pct.library.name_from_metal(channel, dict_name),
                    crange=crange,
                    ax=ax,
                    update_axrange=keepRange==False
                   )
        plt.axis('off')
            #print()

