import anndata
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats
import seaborn as sns
import sqlalchemy as sa

import spherpro.bromodules.filters as filters
import spherpro.bromodules.plot_base as pltbase
import spherpro.configuration as conf
import spherpro.db as db

"""
TODO: move this to project specific package.
"""


class VariableBaseHelper:
    COL_CHANNELNAME = db.ref_planes.channel_name.key
    COL_CONDID = db.conditions.condition_id.key
    COL_CONDLEVEL = COL_CONDID + "level"
    COL_CONDNAME = db.conditions.condition_name.key
    COL_FILTERVAL = db.object_filters.filter_value.key
    COL_GOODNAME = "goodname"
    COL_IMGID = db.images.image_id.key
    COL_IMGLEVEL = COL_IMGID + "level"
    COL_IMID = db.images.image_id.key
    COL_ISNB = "isnb"
    COL_MEASID = db.measurements.measurement_id.key
    COL_MEASID = db.measurements.measurement_id.key
    COL_MEASNAME = db.measurement_names.measurement_name.key
    COL_MEASTYPE = db.measurements.measurement_type.key
    COL_OBJID = db.objects.object_id.key
    COL_SITEID = db.sites.site_id.key
    COL_SITELEVEL = COL_SITEID + "level"
    COL_VALUE = db.object_measurements.value.key
    COL_WORKING = "working"
    COL_D2RIM = "distrim"
    COL_PLATEID = db.conditions.plate_id.key
    COL_PLATELEVEL = COL_PLATEID + "level"


V = VariableBaseHelper


def cur_transf(x):
    return np.log10(x + 0.1)


class HelperVZ(pltbase.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)
        self.filters = filters.Filters(bro)

    def get_pannelcsv(self):
        dat_pannelcsv = self.bro.data.pannel.drop_duplicates(subset="metal")
        return dat_pannelcsv

    def get_measuremeta(
        self,
        dat_pannelcsv,
        measurement_names=None,
        additional_measfilt=None,
        stack_names=None,
    ):
        if measurement_names is None:
            measurement_names = ["MeanIntensityComp", "NbMeanMeanIntensityComp"]
        if stack_names is None:
            stack_names = ["FullStackFiltered"]

        fil_measurements = sa.or_(
            sa.and_(
                db.stacks.stack_name.in_(stack_names),
                db.measurements.measurement_name.in_(measurement_names),
            ),
            sa.and_(
                db.stacks.stack_name == "ObjectStack",
                db.measurements.measurement_name == "dist-rim",
                db.ref_planes.channel_name == "object",
            ),
        )
        if additional_measfilt is not None:
            fil_measurements = sa.or_(fil_measurements, additional_measfilt)
        q = (
            self.bro.session.query(
                db.measurements,
                db.ref_planes.channel_name,
                db.stacks.stack_name,
                db.pannel,
            )
            .join(db.planes, db.measurements.plane_id == db.planes.plane_id)
            .join(db.stacks)
            .join(db.ref_planes)
            .filter(fil_measurements)
            .outerjoin(db.pannel, db.pannel.metal == db.ref_planes.channel_name)
        )
        dat_measmeta = self.bro.doquery(q)

        dat_measmeta = dat_measmeta.merge(
            dat_pannelcsv.loc[:, [db.pannel.metal.key, V.COL_GOODNAME, V.COL_WORKING]],
            on=db.pannel.metal.key,
            how="left",
        )
        dat_measmeta[V.COL_WORKING] = dat_measmeta[V.COL_WORKING].fillna(1)
        dat_measmeta[V.COL_ISNB] = dat_measmeta[V.COL_MEASNAME].map(
            lambda x: "Nb" if x.startswith("Nb") else "Int"
        )
        fil = dat_measmeta[V.COL_GOODNAME].isnull()
        dat_measmeta.loc[fil, V.COL_GOODNAME] = dat_measmeta.loc[fil, V.COL_CHANNELNAME]
        # dat_measmeta = dat_measmeta.set_index(V.COL_MEASID, drop=False)
        return dat_measmeta

    def get_imgmeta(self):
        q = (
            self.bro.session.query(
                db.images, db.conditions.condition_name, db.sites.site_id
            )
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

    def get_data(
        self,
        curcond=None,
        fil_good_meas=None,
        cond_ids=None,
        meas_ids=None,
        object_type=None,
        session=None,
        img_ids=True,
        obj_filter_query=None,
        legacy=True,
    ):

        q_obj = self.data.get_objectmeta_query(session=session)
        if img_ids:
            q_obj = q_obj.add_columns(db.objects.image_id)
        if (curcond is not None) or (cond_ids is not None):
            q_obj = q_obj.join(
                db.conditions, db.images.condition_id == db.conditions.condition_id
            )
            if curcond is not None:
                q_obj = q_obj.filter(db.conditions.condition_name == curcond)

            if cond_ids is not None:
                q_obj = q_obj.filter(db.conditions.condition_id.in_(cond_ids))
        if object_type is not None:
            q_obj = q_obj.filter(db.objects.object_type == object_type)

        if obj_filter_query is not None:
            q_obj = q_obj.filter(db.objects.object_id == obj_filter_query.c.object_id)
        q_meta = self.data.get_measmeta_query(session=session).with_entities(
            db.measurements.measurement_id, db.ref_stacks.scale
        )
        if fil_good_meas is not None:
            q_meta = q_meta.filter(fil_good_meas)
        if meas_ids is not None:
            q_meta = q_meta.filter(db.measurements.measurement_id.in_(meas_ids))
        dat_meas = self.bro.doquery(q_meta)
        dat_obj = self.bro.doquery(q_obj)

        dat = self.bro.io.objmeasurements.get_measurements(dat_obj, dat_meas)
        dat = self.bro.io.objmeasurements.scale_anndata(dat)

        if legacy:
            d = pd.DataFrame(
                dat.X, index=dat.obs.object_id, columns=dat.var.measurement_id
            ).stack()
            d.name = V.COL_VALUE
            dat = d.reset_index().merge(dat_obj)
        return dat

    def get_d2rim(self):
        measdict = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.CORRDIST]
        obj = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.OBJECTTYPE]
        fil = self.filters.measurements.get_measmeta_filter_statements(
            channel_names=[measdict[conf.DEFAULT_CHANNEL_NAME]],
            stack_names=[measdict[conf.DEFAULT_STACK_NAME]],
            measurement_names=[measdict[conf.DEFAULT_MEASUREMENT_NAME]],
            measurement_types=[measdict[conf.DEFAULT_MEASUREMENT_TYPE]],
        )
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
        d = self.bro.doquery(
            self.bro.session.query(
                db.object_filters.object_id,
                db.object_filters.filter_value,
                db.object_filter_names.object_filter_name,
            )
            .join(db.object_filter_names)
            .filter(db.object_filter_names.object_filter_name.in_(filnames))
        )
        d = (
            d.assign(
                **{
                    db.object_filters.filter_value.key: lambda x: x[
                        db.object_filters.filter_value.key
                    ]
                }
            )
            .pivot(
                values=db.object_filters.filter_value.key,
                index=db.object_filters.object_id.key,
                columns=db.object_filter_names.object_filter_name.key,
            )
            .reset_index()
        )
        if outnames is not None:
            d = d.rename({f: o for f, o in zip(filnames, outnames)}, axis=1)
        return d

    def get_sitemeta(self):
        q = (
            self.bro.session.query(
                db.sites, db.acquisitions, db.slideacs, db.slides, db.sampleblocks
            )
            .join(db.acquisitions)
            .join(db.slideacs)
            .join(db.slides)
            .join(db.sampleblocks)
        )
        dat_sitemeta = self.bro.doquery(q)
        dat_sitemeta = dat_sitemeta.loc[:, ~dat_sitemeta.columns.duplicated()]
        return dat_sitemeta

    def plt_clustmatp(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.pivot_table(
            values=V.COL_VALUES, index=V.COL_OBJID, columns=V.COL_MEASID
        )

        corrdat, pval = stats.spearmanr(dat_meas.dropna())
        colnames = dat_measmeta.loc[dat_meas.columns.values, :].apply(
            lambda x: " - ".join([x[V.COL_ISNB], x[V.COL_GOODNAME], x[V.COL_CHANNELN]]),
            axis=1,
        )

        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def transf_intensities(self, dat, dat_measmeta):
        V.COL_VALUE = db.object_measurements.value.key
        ids = dat_measmeta.loc[
            dat_measmeta[V.COL_MEASTYPE] == "Intensity", V.COL_MEASID
        ]
        fil = dat[V.COL_MEASID].isin(ids)
        dat.loc[fil, V.COL_VALUE] = cur_transf(dat.loc[fil, V.COL_VALUE].values)
        return dat

    def plt_clustmatp_pearson(self, dat_meas_raw, dat_measmeta):
        dat_meas = dat_meas_raw.copy()
        dat_meas = self.transf_intensities(dat_meas, dat_measmeta)
        dat_meas = dat_meas.pivot_table(
            values=V.COL_VALUES, index=V.COL_OBJID, columns=V.COL_MEASID
        )
        dat_meas = dat_meas.dropna()
        corrdat = np.corrcoef(dat_meas.T)
        colnames = dat_measmeta.loc[dat_meas.columns.values, :].apply(
            lambda x: " - ".join([x[V.COL_ISNB], x[V.COL_GOODNAME], x[V.COL_CHANNELN]]),
            axis=1,
        )
        cg = sns.clustermap(pd.DataFrame(corrdat, index=colnames, columns=colnames))
        x = plt.setp(cg.ax_heatmap.yaxis.get_majorticklabels(), rotation=0)
        x = plt.setp(cg.ax_heatmap.xaxis.get_majorticklabels(), rotation=90)
        cg.fig.subplots_adjust(bottom=0.3)
        cg.fig.subplots_adjust(right=0.7)
        return cg

    def get_fil_good_meas(self, dat_measmeta, col_working=V.COL_WORKING):
        fil_good_meas = db.measurements.measurement_id.in_(
            [
                int(i)
                for i in dat_measmeta.loc[dat_measmeta[col_working] == 1, V.COL_MEASID]
            ]
        )
        return fil_good_meas

    def get_imgs_for_cond(self, condition_name):
        q = (
            self.bro.session.query(db.images.image_id)
            .join(db.conditions)
            .join(db.valid_images)
            .filter(db.conditions.condition_name == condition_name)
        )
        return [i[0] for i in q.all()]

    # Things for anndata export
    def get_full_obj_meta(self):
        dat_objmeta = self.get_object_meta()
        q = (
            self.bro.session.query(
                db.images,
                db.conditions,
                db.acquisitions,
                db.sites,
                db.slideacs,
                db.slides,
            )
            .join(db.conditions)
            .join(db.acquisitions)
            .join(db.sites)
            .join(db.slideacs)
            .join(db.slides)
            .join(db.valid_images)
        )
        dat_condition = self.bro.doquery(q)
        self.bro.data._read_experiment_layout()
        dat_d2rim = self.get_d2rim()
        dat_full_objmeta = (
            dat_condition.merge(self.bro.data.experiment_layout)
            .pipe(remove_duplicated_columns)
            .merge(dat_objmeta)
            .merge(dat_d2rim)
            .set_index(db.objects.object_id.key, drop=True)
        )
        return dat_full_objmeta

    def get_full_meas_meta(self):
        dat_measmeta = self.bro.doquery(
            self.bro.data.get_measmeta_query().add_columns(
                db.stacks.stack_name, db.ref_planes.channel_name
            )
        )
        dat_full_measmeta = (
            dat_measmeta.merge(
                self.bro.data.pannel,
                left_on=db.ref_planes.channel_name.key,
                right_on="metal",
                how="left",
            )
            .pipe(remove_duplicated_columns)
            .set_index(db.measurements.measurement_id.key, drop=True)
        )
        return dat_full_measmeta

    def convert_to_anndata(self, dat_meas, dat_obs, dat_var):
        dat_meas = dat_meas.pivot_table(
            values=db.object_measurements.value.key,
            index=db.objects.object_id.key,
            columns=db.measurements.measurement_id.key,
        )
        dat = anndata.AnnData(
            dat_meas,
            obs=dat_obs.loc[dat_meas.index, :],
            var=dat_var.loc[dat_meas.columns, :],
        )
        return dat


def get_level(dat, idcol):
    return dat[idcol].map(lambda x: f"{idcol}_{int(x)}")


def rename_measurement(dat, name):
    dat = dat.rename({V.COL_VALUE: name}, axis=1)
    dat = dat.drop(V.COL_MEASID, axis=1)
    return dat


def remove_duplicated_columns(dat):
    return dat.loc[:, ~dat.columns.duplicated()]


class Renamer(object):
    """
    A class to rename & unrename integer varaible names to make them compatible with the linear model
    """

    def __init__(self):
        self.d = dict()

    def rename(self, x):
        rx = "c" + str(x)
        self.d.update({rx: x})
        return rx

    def unrename(self, x):
        return self.d.get(x, x)
