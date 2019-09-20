import spherpro.bromodules.plot_base as pltbase
import spherpro.bromodules.filters as filters
import spherpro.db as db
import spherpro.configuration as conf
import pandas as pd
import numpy as np
import plotnine as gg
import seaborn as sns
import matplotlib.pyplot as plt

import scipy.stats as stats

import sqlalchemy as sa

class VariableBaseHelper:
    COL_CHANNELNAME = db.ref_planes.channel_name.key
    COL_CONDID = db.conditions.condition_id.key
    COL_CONDLEVEL = COL_CONDID + 'level'
    COL_CONDNAME = db.conditions.condition_name.key
    COL_FILTERVAL = db.object_filters.filter_value.key
    COL_GOODNAME = 'goodname'
    COL_IMGID = db.images.image_id.key
    COL_IMGLEVEL = COL_IMGID + 'level'
    COL_IMID = db.images.image_id.key
    COL_ISNB = 'isnb'
    COL_MEASID = db.measurements.measurement_id.key
    COL_MEASID = db.measurements.measurement_id.key
    COL_MEASNAME = db.measurement_names.measurement_name.key
    COL_MEASTYPE = db.measurements.measurement_type.key
    COL_OBJID = db.objects.object_id.key
    COL_SITEID  = db.sites.site_id.key
    COL_SITELEVEL = COL_SITEID + 'level'
    COL_VALUE = db.object_measurements.value.key
    COL_WORKING = 'working'
    COL_D2RIM = 'distrim'
    COL_PLATEID = db.conditions.plate_id.key
    COL_PLATELEVEL = COL_PLATEID+'level'

V = VariableBaseHelper


def cur_transf(x):
    return np.log10(x+0.1)

class HelperVZ(pltbase.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        self.filters = filters.Filters(bro)

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
            .join(db.planes, db.measurements.plane_id==db.planes.plane_id)
            .join(db.stacks)
            .join(db.ref_planes)
            .filter(fil_measurements)
            .outerjoin(db.pannel, db.pannel.metal == db.ref_planes.channel_name)

        )
        dat_measmeta = self.bro.doquery(q)

        dat_measmeta = dat_measmeta.merge(dat_pannelcsv.loc[:, [db.pannel.metal.key, V.COL_GOODNAME, V.COL_WORKING]]
                                                            , on=db.pannel.metal.key, how='left')
        dat_measmeta[V.COL_WORKING] = dat_measmeta[V.COL_WORKING].fillna(1)
        dat_measmeta[V.COL_ISNB] = dat_measmeta[V.COL_MEASNAME].map(lambda x: 'Nb' if x.startswith('Nb') else 'Int')
        fil = dat_measmeta[V.COL_GOODNAME].isnull()
        dat_measmeta.loc[fil, V.COL_GOODNAME] = dat_measmeta.loc[fil, V.COL_CHANNELNAME]
        #dat_measmeta = dat_measmeta.set_index(V.COL_MEASID, drop=False)
        return dat_measmeta

    def get_imgmeta(self):
        q = (self.bro.session.query(db.images, db.conditions.condition_name, db.sites.site_id)
            .join(db.conditions)
            .join(db.acquisitions)
            .join(db.sites)
            .join(db.valid_images)
        )
        dat_imgmeta = self.bro.doquery(q)
        dat_imgmeta[V.COL_SITELEVEL] = get_level(dat_imgmeta, V.COL_SITEID)
        dat_imgmeta[V.COL_CONDLEVEL] = get_level(dat_imgmeta, V.COL_CONDID)
        dat_imgmeta[V.COL_IMGLEVEL] = get_level(dat_imgmeta, V.COL_IMGID)
        return dat_imgmeta

    def get_condmeta(self):
        dat = self.bro.doquery(self.bro.session.query(db.conditions))
        dat[V.COL_PLATELEVEL] = get_level(dat, V.COL_PLATEID)
        return dat

    def get_data(self, curcond=None, fil_good_meas=None, cond_ids=None,
                 meas_ids=None, object_type=None, session=None,
                 img_ids=True, obj_filter_query=None):
        q = (self.data.get_measurement_query(session=session)
             )
        if img_ids:
            q = q.add_columns(db.images.image_id)
        if (curcond is not None) or (cond_ids is not None):
            q = q.join(db.conditions,
                    db.images.condition_id == db.conditions.condition_id)
            if curcond is not None:
                q = q.filter(db.conditions.condition_name == curcond)

            if cond_ids is not None:
                q = q.filter(db.conditions.condition_id.in_(cond_ids))
        if object_type is not None:
            q = q.filter(db.objects.object_type==object_type)

        if obj_filter_query is not None:
            q = q.filter(db.objects.object_id == obj_filter_query.c.object_id)
        q_meta = (self.data.get_measmeta_query(session=session)
                  .with_entities(db.measurements.measurement_id)
                  )
        if fil_good_meas is not None:
            q_meta = q_meta.filter(fil_good_meas)
        if meas_ids is not None:
            q_meta = q_meta.filter(db.measurements.measurement_id.in_(meas_ids))
        measids = [m[0] for m in q_meta.all()]
        q = q.filter(db.measurements.measurement_id.in_(measids))
        return self.bro.doquery(q)

    def get_d2rim(self):
        measdict = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.CORRDIST]
        obj = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.OBJECTTYPE]
        fil = self.filters.measurements.get_measmeta_filter_statements(
                 channel_names=[measdict[conf.DEFAULT_CHANNEL_NAME]],
                 stack_names=[measdict[conf.DEFAULT_STACK_NAME]],
            measurement_names=[measdict[conf.DEFAULT_MEASUREMENT_NAME]],
            measurement_types=[measdict[conf.DEFAULT_MEASUREMENT_TYPE]])
        dat = self.get_data(fil_good_meas=fil, object_type=obj)
        dat = rename_measurement(dat, V.COL_D2RIM)
        return dat

    def get_object_meta(self, object_types=None):
        if object_types is None:
            obj = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.OBJECTTYPE]
            object_types = [obj]
        q = self.bro.session.query(db.objects)
        if len(object_types) > 0:
            q = q.filter(db.objects.object_type.in_(object_types))
        return self.bro.doquery(q)

    def get_fildats(self, filnames, outnames=None):
        d = self.bro.doquery(self.bro.session.query(db.object_filters.object_id, db.object_filters.filter_value,
                                        db.object_filter_names.object_filter_name)
                .join(db.object_filter_names)
                .filter(db.object_filter_names.object_filter_name.in_(filnames))
            )
        d = (d.assign(**{db.object_filters.filter_value.key:
                         lambda x: x[db.object_filters.filter_value.key]})
            .pivot(values=db.object_filters.filter_value.key,
                    index=db.object_filters.object_id.key,
                    columns = db.object_filter_names.object_filter_name.key).reset_index())
        if outnames is not None:
            d = d.rename({f: o for f, o in zip(filnames, outnames)}, axis=1)
        return d
    def get_sitemeta(self):
        q = (self.bro.session.query(db.sites, db.acquisitions, db.slideacs, db.slides, db.sampleblocks)
            .join(db.acquisitions)
            .join(db.slideacs)
            .join(db.slides)
            .join(db.sampleblocks)
        )
        dat_sitemeta = self.bro.doquery(q)
        dat_sitemeta = dat_sitemeta.loc[:, ~dat_sitemeta.columns.duplicated()]
        return dat_sitemeta
    def plt_clustmatp(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.pivot_table(values=V.COL_VALUES,
                            index=V.COL_OBJID,
                            columns=V.COL_MEASID)

        corrdat, pval = stats.spearmanr(dat_meas.dropna())
        colnames = dat_measmeta.loc[dat_meas.columns.values,:].apply(
            lambda x: ' - '.join([x[V.COL_ISNB], x[V.COL_GOODNAME], x[V.COL_CHANNELN]]), axis=1)

        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def transf_intensities(self, dat, dat_measmeta):
        V.COL_VALUE = db.object_measurements.value.key
        ids = dat_measmeta.loc[dat_measmeta[V.COL_MEASTYPE] == 'Intensity', V.COL_MEASID]
        fil = dat[V.COL_MEASID].isin(ids)
        dat.loc[fil, V.COL_VALUE] = cur_transf(
            dat.loc[fil, V.COL_VALUE].values)
        return dat

    def plt_clustmatp_pearson(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.copy()
        dat_meas = self.transf_intensities(dat_meas, dat_measmeta)
        dat_meas = dat_meas.pivot_table(values=V.COL_VALUES,
                            index=V.COL_OBJID,
                            columns=V.COL_MEASID)
        dat_meas = dat_meas.dropna()
        corrdat = np.corrcoef(dat_meas.T)
        colnames = dat_measmeta.loc[dat_meas.columns.values,:].apply(
            lambda x: ' - '.join([x[V.COL_ISNB], x[V.COL_GOODNAME], x[V.COL_CHANNELN]]), axis=1)
        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def get_fil_good_meas(self, dat_measmeta, col_working=V.COL_WORKING):
        fil_good_meas = db.measurements.measurement_id.in_(
                [int(i) for i in dat_measmeta.loc[dat_measmeta[col_working] == 1, V.COL_MEASID]])
        return fil_good_meas

    def get_imgs_for_cond(self, condition_name):
        q = (self.bro.session.query(db.images.image_id)
        .join(db.conditions)
        .join(db.valid_images)
        .filter(db.conditions.condition_name == condition_name))
        return [i[0] for i in q.all()]



def get_level(dat, idcol):
    return dat[idcol].map(lambda x: f'{idcol}_{int(x)}')

def rename_measurement(dat, name):
    dat = dat.rename({V.COL_VALUE: name}, axis=1)
    dat = dat.drop(V.COL_MEASID, axis=1)
    return dat


class Renamer(object):
    """
    A class to rename & unrename integer varaible names to make them compatible with the linear model
    """
    def __init__(self):
        self.d = dict()
    def rename(self, x):
        rx='c'+str(x)
        self.d.update({rx: x})
        return rx

    def unrename(self, x):
        return self.d.get(x,x)
