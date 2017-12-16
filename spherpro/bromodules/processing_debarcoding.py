
import pandas as pd
import numpy as np
import re
import operator

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa


NAME_BARCODE = "BCString"
NAME_WELLCOLUMN = "CondID"
NAME_INVALID = "Invalid"

class Debarcode(object):
    """docstring for Debarcode."""
    def __init__(self, bro):
        self.bro = bro
        self.data = bro.data
        self.filter = sp.bromodules.filter_measurements.FilterMeasurements(self.bro)

    def debarcode(self, dist=40):
        """
        Debarcodes the spheres in the dataset using the debarcoding information
        stored in the condition table
        """
        # get information from conditions and build the barcode key
        cond, key = self._get_barcode_key()
        # get all intensities where dist-sphere<dist
        cells = self._get_bc_cells(key, dist)
        # threshold them
        cells = self._treshold_data(cells)
        # debarcode them
        data = self._debarcode_data(key, cond, cells)
        # summarize them
        bc_dic = self._summarize_singlecell_barcodes(data)
        # update them to the Database
        session = self.data.main_session
        for image in bc_dic.iterrows():
            img = dict(image[1])
            dic = {str(i): str(img[i]) if str(img[i]) != 'NAN' else None for i in img}
            dic[db.images.bc_depth.key] = dist
            session.query(db.images).\
                filter(db.images.image_id == int(image[0])).\
                update(dic)
        session.commit()



    def _treshold_data(self, bc_dat):
        bc_dat = bc_dat.copy()
        bc_dat = bc_dat.apply(lambda x: (x-np.mean(x))/np.std(x),)
        bc_dat[bc_dat > 0] = 1
        bc_dat[bc_dat < 0] = 0
        return bc_dat

    def _debarcode_data(self, bc_key, cond, bc_dat):
        metals = list(bc_key.columns)
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
            temp[db.conditions.condition_id.key]='NAN'
            temp[db.images.bc_valid.key]=0
            temp[db.images.bc_highest_count.key]=0
            temp[db.images.bc_second_count.key]=0

        return pd.Series(temp)



    def _get_barcode_key(self):
        cond = pd.read_sql_query(self.data.main_session.query(db.conditions).statement, self.data.db_conn)
        i = []
        cond[db.conditions.barcode.key].apply(lambda x: i.append(eval(x)))
        key = pd.DataFrame(i)
        cond = pd.concat([cond, key], axis=1)
        return cond, key



    def _get_bc_cells(self, key, dist):
        channels = key.columns.tolist()
        filtdict = {
            db.stacks.stack_name.key: "DistStack",
            db.ref_planes.channel_name.key: "dist-sphere",
            db.measurement_names.measurement_name.key: "MeanIntensity"
        }
        bcfilt = self.filter.get_multifilter_statement([
            (filtdict, operator.lt, dist)
        ])
        measurements = []
        for metal in channels:
            bc_q  = (self.data.get_measurement_query()
                         .filter(
                             self.bro.filters.measurements.get_measurement_filter_statements(
                                 channel_names=[metal],
                                 object_types=['cell'],
                                 stack_names=['FullStack'],
                                 measurement_names=['MeanIntensity'],
                                 measurement_types=['Intensity'],
                             ))
                        )
            bccells = pd.read_sql_query(bc_q.filter(bcfilt).statement, self.data.db_conn)
            measurements.append(bccells)

        df = measurements.copy()
        for i, channel in enumerate(df):
            name = channel[db.ref_planes.channel_name.key].unique()[0]
            df[i] = channel[[db.images.image_id.key,db.objects.object_number.key,db.object_measurements.value.key]]
            df[i].columns = [db.images.image_id.key,db.objects.object_number.key, name]
            if i == 0:
                concat = df[i]
            else:
                concat = concat.merge(df[i], on=[db.images.image_id.key,db.objects.object_number.key])

        concat = concat.set_index([db.images.image_id.key,db.objects.object_number.key])
        return concat
