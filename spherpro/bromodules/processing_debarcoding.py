
import pandas as pd
import numpy as np
import operator

import spherpro as sp
import spherpro.configuration as conf
import spherpro.db as db

import plotnine as gg

NAME_BARCODE = "BCString"
NAME_WELLCOLUMN = "CondID"
NAME_INVALID = "Invalid"
KEY_SECOND = db.images.bc_second_count.key
KEY_HIGHEST = db.images.bc_highest_count.key

SS_BARCODE_MEASUREMENT_NAME = 'barcode'
SS_BARCODE_MEASUREMENT_TYPE = 'object'


class Debarcode(object):
    """docstring for Debarcode."""

    def __init__(self, bro):
        self.bro = bro
        self.data = bro.data
        self.filter = sp.bromodules.filter_measurements.FilterMeasurements(self.bro)
        self.defaults_rawdist = bro.data.conf[conf.QUERY_DEFAULTS][conf.RAWDIST]
        self.defaults_channels = bro.data.conf[conf.QUERY_DEFAULTS][conf.CHANNEL_MEASUREMENTS]
        self.DEFAULT_OBJTYPE = bro.data.conf[conf.QUERY_DEFAULTS][conf.DEFAULT_OBJECT_TYPE]
        self.N = 'n'
        self.COL_TYPE = 'coltype'
        self.NOT_VALID = 0
        self.BC_METACOLS = [db.conditions.condition_id,
                            db.conditions.sampleblock_id]
        self.COL_BC_METACOLS = [c.key for c in self.BC_METACOLS]
        self.CELL_METACOLS = [db.objects.object_id,
                              db.images.image_id,
                              db.sampleblocks.sampleblock_id]
        self.COL_CELL_METACOLS = [c.key for c in self.CELL_METACOLS]

    def debarcode(self, dist=None, borderdist=0, fils=None, stack=None, bc_treshs=None,
                  measurement_name=None, transform=None):
        """
        Debarcodes the spheres in the dataset using the debarcoding information
        stored in the condition table
        """
        # get information from conditions and build the barcode key
        key = self._get_barcode_key()
        # get all intensities where dist-sphere<dist
        dat_cells = self._get_bc_cells(key, dist, fils=fils, borderdist=borderdist, stack=stack,
                                       measurement_name=measurement_name)
        # threshold them
        dat_tresh = self._treshold_data(dat_cells, bc_treshs, transform)
        # debarcode them
        dat_db = self._debarcode_data(key, dat_tresh)
        # summarize them
        dat_sum = self._summarize_singlecell_barcodes(dat_db)
        # calculate statistics
        dat_stat = self._get_barcode_statistics(dat_sum)
        # update them to the Database
        self._write_bc(dat_stat, dist)
        # write single cell barcodes to the Database
        self._write_singlecell_barcodes(dat_db)

    def plot_histograms(self, dist=None, borderdist=0, fils=None, stack=None,
                        measurement_name=None, transform='value'):
        """
        Plot the histograms of the raw data
        """
        key = self._get_barcode_key()
        # get all intensities where dist-sphere<dist
        bcdat = self._get_bc_cells(key, dist, fils=fils, borderdist=borderdist,
                                   stack=stack, measurement_name=measurement_name,
                                   additional_meta=[db.sites.site_id])
        bcvals = bcdat.stack()
        bcvals.name = 'value'
        bcvals = bcvals.reset_index()
        bcvals['site'] = bcvals['site_id'].map(str)
        p = (gg.ggplot(bcvals, gg.aes(x=transform, color='site')) +
             gg.facet_wrap('channel_name', scales='free') +
             gg.geom_density())
        return(p)

    def _write_bc(self, dat_bcstat, dist):
        """
        Writes the barcodes to the database
        """
        session = self.data.main_session

        # assert that debarcoding information from images not debarcoded
        # is deleted
        dat = dat_bcstat.set_index(db.images.image_id.key, drop=False)

        imgids = [i[0] for i in session.query(db.images.image_id).all()]
        dat = dat.loc[imgids, :]
        for row in dat.to_dict(orient='records'):
            # convert all dtypes to int
            row.update({str(c): int(v) if np.isfinite(v) else None for c, v in row.items()})

            if row[db.images.condition_id.key] == self.NOT_VALID:
                # FIXTHIS: coding the invalid condition_id as 0 is dangerous!
                # However I do not know any better way to do this at the moment...
                row[db.images.condition_id.key] = None

            row[db.images.bc_depth.key] = dist
            session.query(db.images).\
                filter(db.images.image_id == row[db.images.image_id.key]).\
                update(row)
        session.commit()

    @staticmethod
    def _default_treshfun(x):
        return (x-np.mean(x) > 0)

    def _treshold_data(self, bc_dat, bc_tresh=None, transform=None,
                       meta_group=None, tresh_fun=None):
        bc_dat = bc_dat.copy()
        if transform is not None:
            bc_dat = bc_dat.transform(transform)

        if tresh_fun is None:
            def tresh_fun(x):
                return (x-np.mean(x)) > 0

        if bc_tresh is None:
            if meta_group is not None:
                bc_dat = bc_dat.groupby(by=meta_group)
            bc_dat = bc_dat.apply(tresh_fun)
        else:
            t = np.array([bc_tresh[c] for c in bc_dat.columns])
            bc_dat = bc_dat.apply(lambda x: x > t, axis=1)
        bc_dat = bc_dat.astype(int)
        return bc_dat

    def _debarcode_data(self, bc_key, dat_tresh):
        meta_cols_bc = self.COL_BC_METACOLS
        meta_cols_cell = self.COL_CELL_METACOLS
        dat_db = (bc_key
                  .reset_index(drop=False)
                  .merge(dat_tresh.reset_index(drop=False), how='right',
                         )
                  .fillna(self.NOT_VALID)
                  .loc[:, set(meta_cols_bc + meta_cols_cell)]
                  )
        return dat_db

    def _summarize_singlecell_barcodes(self, dat_db):
        bc_meta_cols = self.COL_BC_METACOLS
        dat_sum = (dat_db
                   .groupby(by=bc_meta_cols + [db.images.image_id.key])
                   .size().rename(self.N)
                   .reset_index(drop=False)
                   )
        return dat_sum

    def _get_barcode_statistics(self, dat_sum):
        dat_bcstat = (dat_sum
                      .groupby(db.images.image_id.key)
                      .apply(self._aggregate_barcodes)
                      .reset_index(drop=True)
                      .pivot_table(values=self.N, index=[db.images.image_id.key, db.conditions.condition_id.key],
                                   columns=self.COL_TYPE,
                                   fill_value=0)
                      .reset_index(drop=False))
        return dat_bcstat

    def _aggregate_barcodes(self, dat):
        coltypelargest = [db.images.bc_highest_count.key,
                          db.images.bc_second_count.key]
        fil = dat[db.conditions.condition_id.key] != self.NOT_VALID
        d_large = dat.loc[fil].nlargest(2, self.N).copy()
        d_large[self.COL_TYPE] = coltypelargest[:d_large.shape[0]]
        d_none = dat.loc[fil == False, :].copy()
        d_none[self.COL_TYPE] = db.images.bc_invalid.key
        d_sum = (dat.loc[fil].groupby(db.images.image_id.key)[self.N]
                 .sum()
                 .rename(self.N)
                 .reset_index(drop=False)
                 )
        d_sum[self.COL_TYPE] = db.images.bc_valid.key
        d_out = pd.concat([d_large, d_none, d_sum], sort=True)
        if d_large.shape[0] > 0:
            cond = d_large[db.conditions.condition_id.key].iloc[0]
        else:
            cond = self.NOT_VALID
        d_out[db.conditions.condition_id.key] = cond
        return d_out

    def _get_barcode_key(self):
        bro = self.bro
        cond = bro.doquery(bro.session.query(*self.BC_METACOLS,
                                             db.conditions.barcode))
        cond = cond.set_index(self.COL_BC_METACOLS)
        key = cond[db.conditions.barcode.key].apply(lambda x: pd.Series(eval(x)))
        return key

    def _get_bc_cells(self, key, dist=None, fils=None, borderdist=0, stack=None, measurement_name=None,
                      additional_meta=None):
        """
        Get cells for debarcoding
        Args:
            key: the barcoding key
            dist: the max distance to rim of the cells
            borderdist: the max distance, 0=only cells inside the sphere
            stack: the stack
            measurement_name: the measurement name
            additional_meta: a list of additional sqlalchemy columns to be added.
        returns:
            dat_bc: the single cell data
            dat_objmeta: the object metadata
            dat_meas: the measurment metadata
        """

        bro = self.bro
        if measurement_name is None:
            measurement_name = self.defaults_channels[conf.DEFAULT_MEASUREMENT_NAME]
        if stack is None:
            measurement_name = self.defaults_channels[conf.DEFAULT_STACK_NAME]

        cols_meta = self.COL_CELL_METACOLS
        channels = tuple(key.columns.tolist())

        # Generate the measurement dict
        d_rawdist = self.defaults_rawdist
        filtdict = {
            db.stacks.stack_name.key: d_rawdist[conf.DEFAULT_STACK_NAME],
            db.ref_planes.channel_name.key: d_rawdist[conf.DEFAULT_CHANNEL_NAME],
            db.measurement_names.measurement_name.key: d_rawdist[conf.DEFAULT_MEASUREMENT_NAME]
        }
        # Get the distance filters
        distfils = [
            (filtdict, operator.gt, borderdist)]
        if dist is not None:
            distfils+= [(filtdict, operator.lt, dist)]
        fil_dist = self.filter.get_multifilter_statement(distfils)
        # get the filter for the object type
        fil_obj = bro.filters.measurements.get_objectmeta_filter_statements(
                object_types=[self.DEFAULT_OBJTYPE])
        # get the filter for the measurment
        fil_meas = bro.filters.measurements.get_measmeta_filter_statements(
                                     channel_names=[channels],
                                     stack_names=[stack],
                                     measurement_names=[measurement_name],
                                     measurement_types=[None])
        # get the data query
        q_dat = (bro.data.get_measurement_query()
                 .filter(fil_obj)
                 .filter(fil_meas)
                 .filter(fil_dist)
                 .add_columns(db.images.image_id,
                              db.sampleblocks.sampleblock_id
                              )
                 )
        if additional_meta is not None:
            q_dat = q_dat.add_columns(*additional_meta)
            cols_meta = list(set(cols_meta + [c.key for c in additional_meta]))
        # Get the metadata query
        # q_obj = (bro.data.get_objectmeta_query()
        #        .filter(db.objects.object_id == q_dat.subquery().c.object_id)
        #        .with_entities(db.objects.object_id,
        #            db.images.image_id,
        #            db.sampleblocks.sampleblock_id)
        #        )
        # The the measurement metadta query
        q_meas = (bro.data.get_measmeta_query()
                  .filter(fil_meas)
                  .with_entities(db.measurements.measurement_id,
                                 db.ref_planes.channel_name)
                  )
        # Query the data
        dat_cells, dat_measmeta = (bro.doquery(q)
                                   for q in (q_dat, q_meas))

        # bring it into the right form
        # object_meta (in index) ~ metals
        dat_bccells = (dat_cells
                       .merge(dat_measmeta)
                       .pivot_table(values=db.object_measurements.value.key,
                                    columns=db.ref_planes.channel_name.key,
                                    index=cols_meta, aggfunc='mean')
                       )
        return dat_bccells

    def _get_bc_cells_old(self, key, dist, fils=None, borderdist=0, stack=None, measurement_name=None):
        if measurement_name is None:
            measurement_name = 'MeanIntensity'
        if stack is None:
            stack = 'FullStack'
        channels = tuple(key.columns.tolist())
        filtdict = {
            db.stacks.stack_name.key: "DistStack",
            db.ref_planes.channel_name.key: "dist-sphere",
            db.measurement_names.measurement_name.key: "MeanIntensity"
        }
        bcfilt = self.filter.get_multifilter_statement([
            (filtdict, operator.gt, borderdist),
            (filtdict, operator.lt, dist)
        ])
        bc_query = (self.data.get_measurement_query()
                    .filter(
                             self.bro.filters.measurements.get_measurement_filter_statements(
                                 channel_names=[channels],
                                 object_types=['cell'],
                                 stack_names=[stack],
                                 measurement_names=[measurement_name],
                                 measurement_types=['Intensity'],
                             ))
                    .filter(bcfilt)
                    )
        # add additional columns to the output
        bc_query = (bc_query
                    .add_columns(db.ref_planes.channel_name,
                                 db.objects.image_id)
                    )
        bc_query = bc_query.join(db.acquisitions).join(db.sites).add_column(db.sites.site_id)
        if fils is not None:
            bc_query = bc_query.filter(fils)

        dat = self.bro.doquery(bc_query)
        dat = dat.pivot_table(values=db.object_measurements.value.key,
                              columns=db.ref_planes.channel_name.key,
                              index=[db.objects.image_id.key,
                                     db.object_measurements.object_id.key, db.sites.site_id.key])
        return dat

    def _write_singlecell_barcodes(self, dat_db):
        """
        Saves the singlecell barcodes to the database
        """
        mm = self.bro.processing.measurement_maker
        dat_db = dat_db.copy()
        val_obj = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.OBJECT_DEFAULTS]

        plane_id = mm.get_object_plane_id()
        meas_id = mm.register_single_measurement(
            SS_BARCODE_MEASUREMENT_NAME,
            SS_BARCODE_MEASUREMENT_TYPE,
            plane_id)
        dat_db[db.measurements.measurement_id.key] = meas_id
        dat_db = dat_db.rename(columns={db.conditions.condition_id.key: db.object_measurements.value.key})
        self.bro.processing.measurement_maker.add_object_measurements(dat_db, drop_all_old=True)

