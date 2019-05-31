
import pandas as pd
import numpy as np
import re
import operator

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import plotnine as gg

NAME_BARCODE = "BCString"
NAME_WELLCOLUMN = "CondID"
NAME_INVALID = "Invalid"
KEY_SECOND =  db.images.bc_second_count.key
KEY_HIGHEST =  db.images.bc_highest_count.key
DEFAULT_MEASURENAME = 'MeanIntensity'

class Debarcode(object):
    """docstring for Debarcode."""
    def __init__(self, bro):
        self.bro = bro
        self.data = bro.data
        self.filter = sp.bromodules.filter_measurements.FilterMeasurements(self.bro)

    def debarcode(self, dist=40, borderdist=0, fils=None, stack=None, bc_treshs = None,
                  measurement_name=None, transform=None):
        """
        Debarcodes the spheres in the dataset using the debarcoding information
        stored in the condition table
        """
        # get information from conditions and build the barcode key
        cond, key = self._get_barcode_key()
        # get all intensities where dist-sphere<dist
        cells = self._get_bc_cells(key, dist, fils=fils, borderdist=0, stack=stack,
                                   measurement_name=measurement_name)
        # threshold them
        cells = self._treshold_data(cells, bc_treshs, transform)
        # debarcode them
        data = self._debarcode_data(key, cond, cells)
        # summarize them
        bc_dic = self._summarize_singlecell_barcodes(data)
        # update them to the Database
        self._write_bc(bc_dic, dist)

    def plot_histograms(self, dist=40, borderdist=0, fils=None, stack=None,
                        measurement_name=None, transform='value'):
        """
        Plot the histograms of the raw data
        """
        cond, key = self._get_barcode_key()
        # get all intensities where dist-sphere<dist
        bcdat = self._get_bc_cells(key, dist, fils=fils, borderdist=0,
                                   stack=stack, measurement_name=measurement_name)
        bcvals = bcdat.stack()
        bcvals.name = 'value'
        bcvals = bcvals.reset_index('channel_name')
        bcvals['site'] = bcvals.index.get_level_values('site_id').map(str)
        p = (gg.ggplot(bcvals, gg.aes(x=transform, color='site'))+
          gg.facet_wrap('channel_name', scales='free')+
          gg.geom_density())
        return(p)

    def _write_bc(self, bc_dict, dist):
        """
        Writes the barcodes to the database
        """
        session = self.data.main_session
        all_imgs = [i[0] for i in session.query(db.images.image_id).all()]
        bc_dict = bc_dict.reindex(all_imgs, axis=0, fill_value=0)
        bc_dict.loc[:, KEY_SECOND]  = bc_dict[KEY_SECOND].fillna(0)
        bc_dict.loc[:, KEY_HIGHEST]  = bc_dict[KEY_HIGHEST].fillna(0)

        for image in bc_dict.iterrows():
            img = dict(image[1])
            dic = {str(i): int(img[i]) for i in img}

            if dic[db.images.condition_id.key] == 0:
                # FIXTHIS: coding the invalid condition_id as 0 is dangerous!
                # However I do not know any better way to do this at the moment...
                dic[db.images.condition_id.key] = None

            dic[db.images.bc_depth.key] = dist
            session.query(db.images).\
                filter(db.images.image_id == int(image[0])).\
                update(dic)
        session.commit()

    def _treshold_data(self, bc_dat, bc_tresh=None, transform=None):
        bc_dat = bc_dat.copy()
        if transform is not None:
            bc_dat = bc_dat.transform(transform)

        if bc_tresh is None:
            bc_dat = bc_dat.apply(lambda x: (x-np.mean(x)))
            bc_dat[bc_dat > 0] = 1
            bc_dat[bc_dat < 0] = 0
        else:
            t = np.array([bc_tresh[c] for c in bc_dat.columns])
            bc_dat = bc_dat.apply(lambda x: x > t, axis=1)
        return bc_dat

    def _debarcode_data(self, bc_key, cond, bc_dat):
        metals = list(bc_key.columns)
        bc_key = bc_key.copy()
        bc_key[NAME_BARCODE] = bc_key.apply(lambda x: ''.join([str(int(v)) for v in x]),axis=1)
        bc_key = bc_key.reset_index(drop=False)
        bc_dict = cond.merge(bc_key)

        data = bc_dat.loc[:, metals].copy()
        data = data.rename(columns={chan: ''.join(c for c in chan if c.isdigit()) for chan in data.columns})
        data[NAME_BARCODE] = data.apply(lambda x: ''.join([str(int(v)) for v in x]),axis=1)
        bc_dict = bc_dict.set_index(NAME_BARCODE)
        data[NAME_WELLCOLUMN] = [bc_dict[db.conditions.condition_id.key].get(b, NAME_INVALID) for b in data[NAME_BARCODE]]
        bc_dict = bc_dict.reset_index(drop=False)

        data = data.set_index(NAME_WELLCOLUMN, append=True)
        data = data.set_index(NAME_BARCODE, append=True)

        data['count'] = 1

        data = data.loc[data.index.get_level_values(NAME_WELLCOLUMN) != '' ,'count']
        data = data.reset_index(drop=False, level=[NAME_WELLCOLUMN, NAME_BARCODE], name='present')

        return data

    def _summarize_singlecell_barcodes(self, data):
        # prepare a dicitionary containing the barcode
        idxs = data.index.get_level_values(db.images.image_id.key).unique()
        dic = data.groupby(level=db.images.image_id.key).apply(self._aggregate_barcodes)
        return dic


    def _aggregate_barcodes(self, dat):
        temp = dict()
        summary = dat[NAME_WELLCOLUMN].value_counts()
        try:
            temp[db.images.bc_invalid.key]=summary[NAME_INVALID]
            del(summary[NAME_INVALID])
        except KeyError:
            temp[db.images.bc_invalid.key]=0
        try:
            temp[db.conditions.condition_id.key]=summary.keys()[0]
            temp[db.images.bc_valid.key]=summary.sum()
            temp[db.images.bc_highest_count.key]=summary[temp[db.conditions.condition_id.key]]
            if len(summary) > 1:
                temp[db.images.bc_second_count.key]=summary[summary.keys()[1]]
            else:
                temp[db.images.bc_second_count.key]=0
        except IndexError:
            temp[db.conditions.condition_id.key]='0'
            temp[db.images.bc_valid.key]=0
            temp[db.images.bc_highest_count.key]=0
            temp[db.images.bc_second_count.key]=0

        return pd.Series(temp)



    def _get_barcode_key(self):
        cond = pd.read_sql_query(self.data.main_session.query(db.conditions).statement, self.data.db_conn)
        cond = cond.set_index(db.conditions.condition_id.key, drop=False)
        key = cond[db.conditions.barcode.key].apply(lambda x: pd.Series(eval(x)))
        return cond, key



    def _get_bc_cells(self, key, dist, fils=None, borderdist=0, stack=None, measurement_name=None):
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
        bc_query  = (self.data.get_measurement_query()
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
