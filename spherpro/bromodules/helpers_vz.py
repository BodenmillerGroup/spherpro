import spherpro.bromodules.plot_base as pltbase
import spherpro.db as db
import spherpro.configuration as conf
import pandas as pd
import numpy as np
import plotnine as gg
import seaborn as sns
import matplotlib.pyplot as plt

import scipy.stats as stats

import sqlalchemy as sa

COL_CHANNELN = db.ref_planes.channel_name.key
COL_MEASNAME = db.measurements.measurement_name.key
COL_VALUES = db.object_measurements.value.key
COL_MEASID = db.measurements.measurement_id.key
COL_IMID = db.images.image_id.key
COL_OBJID = db.objects.object_id.key
COL_SITE  = db.sites.site_id.key
COL_MEASTYPE = db.measurements.measurement_type.key
COL_CONDID = db.conditions.condition_id.key
COL_GOODNAME = 'goodname'
COL_WORKING = 'working'
COL_SITE_LEVEL = 'SiteLevel'
COL_ISNB = 'isnb'

def cur_transf(x):
    return np.log10(x+0.1)

class HelperVZ(pltbase.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)

    def get_pannelcsv(self):
        dat_pannelcsv = self.bro.data.pannel.drop_duplicates(subset='metal')
        return dat_pannelcsv

    def get_measuremeta(self, dat_pannelcsv, measurement_names=None, additional_measfilt=None,
                        stack_names = None):
        if measurement_names is None:
            measurement_names = ['MeanIntensityComp', 'NbMeanMeanIntensityComp']
        if stack_names is None:
            stack_names = ['FullStackFiltered']

        fil_measurements = sa.or_(
            sa.and_(db.stacks.stack_name.in_(stack_names),
                db.measurements.measurement_name.in_(measurement_names)),
            sa.and_(db.stacks.stack_name == 'ObjectStack',
                db.measurements.measurement_name == 'dist-rim',
                db.ref_planes.channel_name == 'object')
        )
        if additional_measfilt is not None:
            fil_measurements = sa.or_(fil_measurements,additional_measfilt)
        q = (self.bro.session.query(db.measurements, db.ref_planes.channel_name, db.stacks.stack_name,
                        db.pannel)
            .join(db.planes)
            .join(db.stacks)
            .join(db.ref_planes)
            .filter(fil_measurements)
            .outerjoin(db.pannel, db.pannel.metal == db.ref_planes.channel_name)

        )
        dat_measmeta = self.bro.doquery(q)

        dat_measmeta = dat_measmeta.merge(dat_pannelcsv.loc[:, [db.pannel.metal.key, COL_GOODNAME, COL_WORKING]]
                                                            , on=db.pannel.metal.key, how='left')
        dat_measmeta[COL_WORKING] = dat_measmeta[COL_WORKING].fillna(1)
        dat_measmeta[COL_ISNB] = dat_measmeta[COL_MEASNAME].map(lambda x: 'Nb' if x.startswith('Nb') else 'Int')
        fil = dat_measmeta[COL_GOODNAME].isnull()
        dat_measmeta.loc[fil, COL_GOODNAME] = dat_measmeta.loc[fil, COL_CHANNELN]
        dat_measmeta = dat_measmeta.set_index(COL_MEASID, drop=False)
        return dat_measmeta

    def get_imgmeta(self):
        q = (self.bro.session.query(db.images, db.conditions.condition_name, db.sites.site_id)
            .join(db.conditions)
            .join(db.acquisitions)
            .join(db.sites)
            .join(db.valid_images)
        )
        dat_imgmeta = self.bro.doquery(q)
        dat_imgmeta = dat_imgmeta.set_index(COL_IMID, drop=False)
        dat_imgmeta[COL_SITE_LEVEL]= dat_imgmeta[COL_SITE].map(str)
        return dat_imgmeta

    def get_data(self, curcond=None, fil_good_meas=None, cond_ids=None,
                 meas_ids=None, object_type=None):
        q = (self.bro.session.query(db.object_measurements, db.images.image_id)
            .join(db.measurements)
            .join(db.objects)
            .join(db.images)
        .join(db.valid_images)
        .join(db.valid_objects)
            .join(db.conditions)
            .join(db.planes)
            .join(db.stacks)
            .join(db.ref_planes)
        )
        if curcond is not None:
            q = q.filter(db.conditions.condition_name == curcond)

        if cond_ids is not None:
            q = q.filter(db.conditions.condition_id.in_(cond_ids))
        if fil_good_meas is not None:
            q = q.filter(fil_good_meas)
        if meas_ids is not None:
            q = q.filter(db.measurements.measurement_id.in_(meas_ids))
        if object_type is not None:
            q = q.filter(db.objects.object_type==object_type)

        return self.bro.doquery(q)

    def plt_clustmatp(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.pivot_table(values=COL_VALUES,
                            index=COL_OBJID,
                            columns=COL_MEASID)

        corrdat, pval = stats.spearmanr(dat_meas.dropna())
        colnames = dat_measmeta.loc[dat_meas.columns.values,:].apply(
            lambda x: ' - '.join([x[COL_ISNB], x[COL_GOODNAME], x[COL_CHANNELN]]), axis=1)

        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def transf_intensities(self, dat, dat_measmeta):
        COL_VALUE = db.object_measurements.value.key
        ids = dat_measmeta.loc[dat_measmeta[COL_MEASTYPE] == 'Intensity', COL_MEASID]
        fil = dat[COL_MEASID].isin(ids)
        dat.loc[fil, COL_VALUE] = cur_transf(
            dat.loc[fil, COL_VALUE].values)
        return dat

    def plt_clustmatp_pearson(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.copy()
        dat_meas = self.transf_intensities(dat_meas, dat_measmeta)
        dat_meas = dat_meas.pivot_table(values=COL_VALUES,
                            index=COL_OBJID,
                            columns=COL_MEASID)
        dat_meas = dat_meas.dropna()
        corrdat = np.corrcoef(dat_meas.T)
        colnames = dat_measmeta.loc[dat_meas.columns.values,:].apply(
            lambda x: ' - '.join([x[COL_ISNB], x[COL_GOODNAME], x[COL_CHANNELN]]), axis=1)
        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def get_fil_good_meas(seklf, dat_measmeta):
        fil_good_meas = db.measurements.measurement_id.in_(
                [int(i) for i in dat_measmeta.loc[dat_measmeta[COL_WORKING] == 1, COL_MEASID]])
        return fil_good_meas

    def get_imgs_for_cond(self, condition_name):
        q = (self.bro.session.query(db.images.image_id)
        .join(db.conditions)
        .join(db.valid_images)
        .filter(db.conditions.condition_name == condition_name))
        return [i[0] for i in q.all()]
