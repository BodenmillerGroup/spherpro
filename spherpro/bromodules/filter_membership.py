import logging

import pandas as pd

import spherpro.bromodules.filter_base as filter_base
import spherpro.bromodules.filter_objectfilters as custfilter
import spherpro.configuration as conf
import spherpro.db as db
import spherpro.library as lib
from spherpro.bromodules.helpers_varia import HelperDb


class FilterMembership(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)
        self.filter_custom = custfilter.ObjectFilterLib(bro)
        self.defaults = self.data.conf[conf.QUERY_DEFAULTS]
        self.helperdb = HelperDb(bro)

    def add_issmall(self, minpix=10, name=None, measid_area=None,
                    object_type=None, drop=True):
        if name is None:
            name = 'is-small'
        obj_def = self.defaults[conf.OBJECT_DEFAULTS]
        measfilts = self.bro.filters.measurements

        if measid_area is None:
            fil = measfilts.get_measmeta_filter_statements(
                channel_names=[obj_def[conf.DEFAULT_CHANNEL_NAME]],
                stack_names=[obj_def[conf.DEFAULT_STACK_NAME]],
                measurement_names=['Area'],
                measurement_types=['AreaShape'])
            measid_area = (self.data.get_measmeta_query()
                           .filter(fil)
                           .with_entities(db.measurements.measurement_id)
                           ).one()[0]
        q_meas = (self.data.get_measmeta_query()
                  .filter(db.measurements.measurement_id == measid_area)
                  .add_columns(db.ref_stacks.scale)
                  )
        q_obj = self.data.get_objectmeta_query()

        if object_type is not None:
            q_obj = q_obj.filter(db.objects.object_type == object_type)

        dat_meas = self.doquery(q_meas)
        dat_obj = self.doquery(q_obj)

        dat = self.bro.io.objmeasurements.get_measurements(dat_obj, dat_meas)
        self.bro.io.objmeasurements.scale_anndata(dat)

        dat_filter = pd.DataFrame({
            db.objects.object_id.key: map(int, dat.obs.index),
            db.object_filters.filter_value.key:
                dat[:, str(measid_area)].X.squeeze() < minpix})
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_ismaincomponent(self, name=None, drop=True,
                            relation='Neighbors',
                            object_type='cell'):
        if name is None:
            name = 'is-maincomponent'
        dat_nb = self.helperdb.get_nb_dat(relation, obj_type=object_type)
        dat_obj = self.bro.doquery(self.session.query(db.objects.object_id, db.objects.image_id)
                                   .join(db.valid_objects)
                                   .filter(db.objects.object_type == object_type)
                                   )
        largest_obj = (dat_nb
                       .merge(dat_obj, left_on=db.object_relations.object_id_parent.key,
                              right_on=db.objects.object_id.key)
                       .groupby(db.images.image_id.key)
                       .apply(lib.get_largest_commponent_objs)
                       )
        dat_obj[db.object_filters.filter_value.key] = \
            dat_obj[db.objects.object_id.key].isin(largest_obj)
        self.filter_custom.write_filter_to_db(dat_obj, name, drop)
        return dat_obj

    def add_issphere(self, minfrac=0.6, name=None, drop=True,
                     object_type='cell'):
        if name is None:
            name = 'is-sphere'
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_isbg = 'is-bg'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        non_zero_offset = 1 / 2 ** 20
        dat_meas = self.doquery(
            self.data.get_measmeta_query()
                .filter(db.measurements.measurement_name == col_measure)
                .filter(db.stacks.stack_name == col_stack)
                .filter(db.ref_planes.channel_name.in_(
                [col_isother, col_issphere, col_isbg]))
                .add_columns(db.ref_stacks.scale,
                             db.ref_planes.channel_name)
        )
        dat_obj = self.doquery(self.data.get_objectmeta_query()
                               .filter(db.objects.object_type == object_type)
                               )
        logging.debug(f'{dat_obj.shape}')

        dat = self.bro.io.objmeasurements.get_measurements(dat_obj, dat_meas)
        logging.debug(f'{dat.shape}')
        self.bro.io.objmeasurements.scale_anndata(dat)

        fil_issphere, fil_isbg, fil_isother = (dat.var[db.ref_planes.channel_name.key] == c
                                               for c in (col_issphere, col_isbg, col_isother))
        dat_filter = pd.DataFrame({
            db.objects.object_id.key: map(int, dat.obs.index),
            db.object_filters.filter_value.key:
                (((dat.X[:, fil_issphere] + non_zero_offset) / (
                        dat.X[:, fil_isother] + dat.X[:, fil_isbg] +
                        dat.X[:, fil_issphere] + non_zero_offset)) > minfrac).astype(int).flatten()})
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_isambiguous(self, distother=-10, name=None, drop=True,
                        object_type='cell'):
        if name is None:
            name = 'is-ambiguous'
        col_measure = 'MeanIntensity'
        col_stack = 'DistStack'
        col_distother = 'dist-other'
        measid = (self.data.get_measmeta_query()
            .filter(db.measurements.measurement_name == col_measure)
            .filter(db.stacks.stack_name == col_stack)
            .filter(db.ref_planes.channel_name == col_distother)
            .with_entities(db.measurements.measurement_id)
            .one()[0]
            )

        q_meas = (self.data.get_measmeta_query()
                  .filter(db.measurements.measurement_id == measid)
                  .add_columns(db.ref_stacks.scale)
                  )
        q_obj = self.data.get_objectmeta_query()

        if object_type is not None:
            q_obj = q_obj.filter(db.objects.object_type == object_type)

        dat_meas = self.doquery(q_meas)
        dat_obj = self.doquery(q_obj)

        dat = self.bro.io.objmeasurements.get_measurements(dat_obj, dat_meas)
        self.bro.io.objmeasurements.scale_anndata(dat)

        d = dat[:, str(measid)].X.squeeze()
        dat_filter = pd.DataFrame({
            db.objects.object_id.key: map(int, dat.obs.index),
            db.object_filters.filter_value.key:
                (d > distother) & (
                        d < (2 ** 16 - 2)).astype(int)})
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter

    def add_isnotborder(self, borderdist=5, name=None, drop=True,
                        object_type='cell'):
        if name is None:
            name = 'is-notborder'
        col_measure = 'MinIntensity'
        col_stack = 'DistStack'
        col_distsphere = 'dist-sphere'

        measid = (self.data.get_measmeta_query()
            .filter(db.measurements.measurement_name == col_measure)
            .filter(db.stacks.stack_name == col_stack)
            .filter(db.ref_planes.channel_name == col_distsphere)
            .with_entities(db.measurements.measurement_id)
            .one()[0]
            )
        q_meas = (self.data.get_measmeta_query()
                  .filter(db.measurements.measurement_id == measid)
                  .add_columns(db.ref_stacks.scale)
                  )
        q_obj = self.data.get_objectmeta_query()

        if object_type is not None:
            q_obj = q_obj.filter(db.objects.object_type == object_type)

        dat_meas = self.doquery(q_meas)
        dat_obj = self.doquery(q_obj)

        dat = self.bro.io.objmeasurements.get_measurements(dat_obj, dat_meas)
        self.bro.io.objmeasurements.scale_anndata(dat)

        d = dat[:, str(measid)].X.squeeze()
        dat_filter = pd.DataFrame({
            db.objects.object_id.key: map(int, dat.obs.index),
            db.object_filters.filter_value.key:
                (d > borderdist) & (
                        d < (2 ** 16 - 2)).astype(int)})
        self.filter_custom.write_filter_to_db(dat_filter, name, drop)
        return dat_filter
