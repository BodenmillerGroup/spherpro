import pandas as pd
import numpy as np
import re
import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa
import matplotlib.pyplot as plt
import matplotlib_scalebar as scalebar

from spherpert import helpers



LABEL_Y = "Condition ID number"
LABEL_X = "Image ID number"
PLT_TITLE = "All images from a single condition"


class PlotConditionImages(plot_base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        # make the dependency explicit
        self.heatmask = bro.plots.heatmask
        self.measurement_filters = bro.filters.measurements
        self.objectfilterlib = bro.filters.objectfilterlib
        self.imcimage = bro.io.imcimg


    def get_target_by_channel(self, channel_name):
        target = (self.session.query(db.pannel.target)
            .filter(db.pannel.metal == channel_name).one_or_none())
        if target is None:
            target = [channel_name]
        return target[0]


    def plot_hm_conditions(self, condition_name, channel_name, minmax=(0,1), transf=None):

        cond_list = self.get_cond_id_im_id(condition_name)
        im_dict = self.get_dict_imgs(cond_list,channel_name)
        if transf is not None:
            for key, val in im_dict.items():
                im_dict[key] = transf(val)

        target = self.get_target_by_channel(channel_name)
        title = 'condition: %s\nchannel: %s - %s' % (condition_name, channel_name, target)

        hm = self.plot_layout(cond_list,im_dict, title, minmax=minmax)

        return hm


    def plot_imc_conditions(self, condition_name, channel_name, minmax=(0,1), transf=None):

        cond_list = self.get_cond_id_im_id(condition_name)
        im_dict = self.get_dict_imc_imgs(cond_list,channel_name)
        if transf is not None:
            for key, val in im_dict.items():
                im_dict[key] = transf(val)

        target = self.get_target_by_channel(channel_name)
        title = 'condition: %s\nchannel: %s - %s' % (condition_name, channel_name, target)

        hm = self.plot_layout(cond_list,im_dict, title, minmax=minmax)

        return hm


    def plot_layout(self, cond_list, im_dict, title, pltfkt=None, minmax=(0,1)):

        if pltfkt is None:
            pltfkt = self.plot_im

        nrows = len(cond_list)
        ncols = max([len(c[1]) for c in cond_list ])

        crange = self.get_crange(im_dict, minmax)

        cond_id, image_id = zip(*cond_list)

        fig, ax = plt.subplots(nrows, ncols, sharex=True, sharey=True, squeeze=True)

        for i, axrow in enumerate(ax):
            cond, images = cond_list[i]

            for j, a in enumerate(axrow):
                if j < len(images):
                    image = images[j]
                    img = im_dict[image]
                    cax = pltfkt(img, ax=a, crange = crange)
                    sb = scalebar.scalebar.ScaleBar(1, units='um', location=4)
                    a.add_artist(sb)
                    a.set_xticks([])
                    a.set_yticks([])
                    a.set_title('Im_id: %s' % str(image),  size='small')
                    if j==0:
                        a.set_ylabel('Cond_id: %s' % str(cond), rotation=0, size='small', labelpad=39)

                else:
                    a.set_visible(False)


        plt.colorbar(cax, ax=ax.ravel().tolist())
        plt.suptitle(title)
        return ax


    @staticmethod
    def plot_im(img, title=None,crange=None, ax=None, update_axrange=True, cmap=None):

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

        cax = ax.imshow(img, cmap=cmap)

        if hasattr(img, 'mask'):
            mask_img = np.isnan(img)
            if np.any(mask_img):
                mask_img = np.ma.array(mask_img, mask=img.mask | (mask_img == False),fill_value=0)
                ax.imshow(mask_img, alpha=0.2)

        cax.set_clim(crange[0], crange[1])
        return cax


    @staticmethod
    def get_crange(img_dict, minmax=(0,1)):
        vals = np.concatenate([v[np.isnan(v) == False] for v in img_dict.values()])
        crange = [np.percentile(vals, 100*minmax[0]), np.percentile(vals, 100*minmax[1])]
        return  crange



    def get_dict_imgs(self, cond_list, channelname):
        imgids = {img: self.get_im_data(str(img),channelname) for c, imgs in cond_list for img in imgs}
        return imgids


    def get_dict_imc_imgs(self, cond_list, channelname):
        imac = {img: self.imcimage.get_imcimg(int(img)) for c, imgs in cond_list for img in imgs}
        for key, val in imac.items():
            imac[key] = val.get_img_by_metal(channelname)
        return imac


    def get_im_data(self, im_num, channelname):

        fil_hq = self.objectfilterlib.get_combined_filterstatement([('is-sphere', True), ('is-ambiguous', False)])

        q = (self.data.get_measurement_query().filter(fil_hq,
                                db.stacks.stack_name == 'FullStackFiltered',
                                db.measurements.measurement_name == 'MeanIntensity',
                                db.images.image_id == im_num,
                                db.ref_planes.channel_name == channelname))

        pdat = self.bro.doquery(q)
        img = self.heatmask.assemble_heatmap_image(pdat)

        return img


    @staticmethod
    def logvalue(val):
        new_val = np.log10(val + 0.00001)

        return new_val



    def get_cond_id_im_id(self, condition_name):

        sq =(self.session.query(db.images.image_id)
        .filter(sa.and_(db.images.bc_highest_count > 10,db.images.bc_highest_count/(db.images.bc_second_count+1) > 2 ))).subquery()
        fil_db = db.images.image_id == sq.c.image_id

        p = (self.session.query(db.objects.image_id,
                                     db.conditions.condition_id,
                                    )
                         .join(db.images)
                         .join(db.conditions)
                         .filter(fil_db,
                                 db.conditions.condition_name == condition_name)
                         )

        pdat = self.bro.doquery(p)

        cond_id_im_id = []
        for cond, conddat in pdat.groupby('condition_id'):
            cond_im = (cond, conddat['image_id'].unique())

            cond_id_im_id.append(cond_im)

        return cond_id_im_id
