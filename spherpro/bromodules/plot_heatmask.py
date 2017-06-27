import spherpro.bromodules.plot_base as plot_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class PlotHeatmask(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        
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
    

def assemble_heatmap_image(cut_ids, cut_slices, cut_masks, dat_cells_channel,
                           cell_id_name = 'ObjectNumber', cut_id_name='ImageNumber', out_shape=None):
    
    
    if out_shape is None:
        x_start, x_stop, y_start, y_stop = zip(*[(slx.start, slx.stop, sly.start, sly.stop) for slx, sly in cut_slices])
        new_shape = (max(x_stop), max(y_stop))
    else:
        new_shape = out_shape
    pimg = np.empty(new_shape)
    pimg[:] = np.NAN
    notbg = np.zeros(new_shape)

    cur_cids = dat_cells_channel.index.get_level_values(cut_id_name).unique()
    for cid, sl, mask in zip(cut_ids, cut_slices, cut_masks):
        if cid in cur_cids:
            intensity = dat_cells_channel.xs(cid, level=cut_id_name)

            timg= pct.library.map_series_on_mask(mask, intensity, label=intensity.index.get_level_values(cell_id_name))
            pimg_sl =pimg[sl]

            fil = (timg.mask == False) & (np.isnan(timg.imag) == False)  &(np.isnan(pimg_sl) == True)
            pimg_sl[fil] = timg[fil]


            fil2 = notbg[sl] == 0
            notbg[sl][fil2] = (timg.mask==False)[fil2]

    pimg = np.ma.array(pimg, mask=(notbg ==0))
    
    if out_shape is None:
        pimg = pimg[min(x_start):, min(y_start):]
    
    return(pimg)

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

