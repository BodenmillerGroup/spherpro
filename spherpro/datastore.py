import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import os
import re
import io
import warnings
from odo import odo
import tifffile as tif

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.configuration as conf
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
import sqlalchemy as sa

DICT_DB_KEYS = {
    'image_number': db.images.image_id.key,
    'object_number': db.objects.object_number.key,
    'measurement_type': db.measurement_types.measurement_type.key,
    'measurement_name': db.measurement_names.measurement_name.key,
    'stack_name': db.stacks.stack_name.key,
    'plane_id': db.ref_planes.ref_plane_id.key,
    'object_id': db.objects.object_id.key
}

OBJECTS_STACKNAME = 'ObjectStack'
OBJECTS_CHANNELNAME = 'object'
OBJECTS_PLANEID = '1'
OBJECTS_CHANNELTYPE = 'object'

class DataStore(object):
    """DataStore
    The DataStore class is intended to be used as a storage for spheroid IMC
    data. It features two Backends, MySQL and SQLite.

    Methods:
        Base:
            read_config: read configfile
            import_data: reads and writes data to the database
            resume_data: reads non-database files and configures backend
    """
    def __init__(self):
        # init empty properties here
        self.experiment_layout = None
        self.barcode_key = None
        self.well_measurements = None
        self.cut_meta = None
        self.roi_meta = None
        self.channel_meta = None
        self.sphere_meta = None
        self.measurement_meta_cache = None
        self._pannel = None
        self._session = None
        self._session_maker = None
        self.connectors = {
            conf.CON_SQLITE: db.connect_sqlite,
            conf.CON_MYSQL: db.connect_mysql,
            conf.CON_POSTGRESQL: db.connect_postgresql
        }

    #########################################################################
    #########################################################################
    #                      Import or Resume functions:                      #
    #########################################################################
    #########################################################################

    def read_config(self, configpath):
        """
        finds the measurement meta information from a given string

        Args:
            configpath: A string denoting the location of the config file

        Raises:
            YAMLError
        """
        self.conf = conf.read_configuration(configpath)

    def import_data(self, minimal=None):
        """read_data
        Reads the Data using the file locations given in the configfile.
        Args:
            minimal: Bool, if True, the import process only imports values from
                the RefStacks and no location values
        """
        if minimal is None:
            minimal = False
        # Read the data based on the config
        self._read_experiment_layout()
        self._read_barcode_key()
        self._read_measurement_data()
        self._read_image_data()
        self._read_relation_data()
        self._read_stack_meta()
        self._populate_db(minimal)

    def resume_data(self):
        """read_data
        Reads non-database files and configures backend according to
        the configfile.
        """
        # Read the data based on the config
        self._read_experiment_layout()
        self._read_barcode_key()
        # self._readWellMeasurements()
        # self._read_cut_meta()
        # self._read_roi_meta()
        #self._read_measurement_data()
        self._read_stack_meta()
        self._read_pannel()
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)

    def drop_all(self):
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)
        db.drop_all(self.db_conn)

    ##########################################
    #   Helper functions used by readData:   #
    ##########################################

    def _read_experiment_layout(self):
        """
        reads the experiment layout as stated in the config
        and saves it in the datastore
        """
        if self.conf[conf.LAYOUT_CSV][conf.PATH] is not None:
            sep = self.conf[conf.LAYOUT_CSV][conf.SEP]
            self.experiment_layout = pd.read_csv(
                self.conf[conf.LAYOUT_CSV][conf.PATH], sep=sep
            )
        else:
            self.experiment_layout = None

    def _read_barcode_key(self):
        """
        reads the barcode key as stated in the config
        and saves it in the datastore
        """
        if self.conf[conf.BARCODE_CSV][conf.PATH] is not None:
            sep = self.conf[conf.BARCODE_CSV][conf.SEP]
            self.barcode_key = pd.read_csv(
                self.conf[conf.BARCODE_CSV][conf.PATH], sep=sep
            ).set_index(
                [
                    self.conf[conf.BARCODE_CSV][conf.BC_CSV_PLATE_NAME],
                    self.conf[conf.BARCODE_CSV][conf.BC_CSV_WELL_NAME]
                ]
            )
        else:
            self.barcode_key = None

    def _read_well_measurements(self):
        """
        reads the well measurement file as stated in the config
        and saves it in the datastore
        """
        raise NotImplementedError


    def _read_cut_meta(self, cutfile):
        """
        reads the cut meta file as stated in the config
        and saves it in the datastore
        """
        raise NotImplementedError

    def _read_roi_meta(self, roifile):
        """
        reads the roi meta as stated in the config
        and saves it in the datastore
        """
        raise NotImplementedError


    def _read_measurement_data(self):
        """
        reads the measurement data as stated in the config
        and saves it in the datastore
        """
        conf_meas = self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV]
        sep = conf_meas[conf.SEP]
        cpdir = self.conf[conf.CP_DIR]
        filetype = conf_meas[conf.FILETYPE]
        objids = conf_meas[conf.OBJECTS]
        cellmeas ={objid: pd.read_csv(os.path.join(cpdir, objid+filetype),
                                      sep=sep) for objid in objids}
        cellmeas = pd.concat(cellmeas, names=[db.objects.object_type.key, 'idx'] )
        cellmeas = cellmeas.reset_index(level=db.objects.object_type.key, drop=False)
        # do renaming
        rename_dict = {
            self.conf[conf.OBJECTNUMBER]: db.objects.object_number.key,
            self.conf[conf.IMAGENUMBER]: db.images.image_number.key}
        self._measurement_csv = cellmeas.rename(columns=rename_dict)

    def _read_image_data(self):
        cpdir = self.conf[conf.CP_DIR]
        rename_dict = {self.conf[conf.IMAGENUMBER]: db.images.image_number.key}
        images_csv = lib.read_csv_from_config(
            self.conf[conf.CPOUTPUT][conf.IMAGES_CSV],
            base_dir=cpdir)
        images_csv = images_csv.rename(columns=rename_dict)
        self._images_csv = images_csv

    def _read_relation_data(self):
        conf_rel = self.conf[conf.CPOUTPUT][conf.RELATION_CSV]
        cpdir = self.conf[conf.CP_DIR]
        relation_csv = lib.read_csv_from_config(
            self.conf[conf.CPOUTPUT][conf.RELATION_CSV],
            base_dir=cpdir)
        col_map = {conf_rel[c]: target for c, target in [
            (conf.OBJECTTYPE_FROM, conf.OBJECTTYPE_FROM),
            (conf.OBJECTTYPE_TO, conf.OBJECTTYPE_TO),
            (conf.OBJECTNUMBER_FROM, conf.OBJECTNUMBER_FROM),
            (conf.OBJECTNUMBER_TO, conf.OBJECTNUMBER_TO),
            (conf.IMAGENUMBER_FROM, conf.IMAGENUMBER_FROM),
            (conf.IMAGENUMBER_TO, conf.IMAGENUMBER_TO),
            (conf.RELATIONSHIP, db.object_relation_types.object_relationtype_name.key)]}

        self._relation_csv = relation_csv.rename(columns=col_map)

    def _read_stack_meta(self):
        """
        reads the stack meta as stated in the config
        and saves it in the datastore
        """
        stack_dir = self.conf[conf.STACK_DIR][conf.PATH]
        sep = self.conf[conf.STACK_DIR][conf.SEP]
        match = re.compile("(.*)\.csv")
        stack_files = [f for f in listdir(stack_dir) if isfile(join(stack_dir, f))]
        stack_data = [pd.read_csv(join(stack_dir,n), sep) for n in stack_files]
        stack_files = [match.match(name).groups()[0] for name in stack_files]
        self.stack_csvs = {stack: data for stack, data in zip(stack_files, stack_data)}
        self._stack_relation_csv = lib.read_csv_from_config(self.conf[conf.STACK_RELATIONS])

    def _read_pannel(self):
        """
        Reads the pannel as stated in the config.
        """
        self._pannel = lib.read_csv_from_config(self.conf[conf.PANNEL_CSV])


    def _populate_db(self, minimal):
        """
        Writes the tables to the database
        """
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)
        self.drop_all()
        db.initialize_database(self.db_conn)
        self._write_image_table()
        self._write_masks_table()
        self._write_objects_table()
        self._write_stack_tables()
        self._write_refplanes_table()
        self._write_planes_table()
        self._write_pannel_table()
        self._write_condition_table()
        self._write_site_table()
        self._write_measurement_table(minimal)
        self._write_object_relations_table()
        # vacuum after population in postgres
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            self._pg_vacuum()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _write_stack_tables(self):
        """
        Creates the StackModifications, StackRelations, Modifications,
        RefStack and DerivedStack tables and writes them to the database
        """

        # Modifications
        modifications = self._generate_modifications()
        self._bulkinsert(modifications, db.modifications)

        # RefStacks
        RefStack = self._generate_refstack()
        self._bulkinsert(RefStack, db.ref_stacks)

        # Stacks
        Stack = self._generate_stack()
        self._bulkinsert(Stack, db.stacks)

        # StackModifications
        stackmodification = self._generate_stackmodification()
        self._bulkinsert(stackmodification, db.stack_modifications)




    def _generate_modifications(self):
        """
        Generates the modification table
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        modpre_col = self.conf[conf.STACK_RELATIONS][conf.MODPRE]
        stackrel = self._stack_relation_csv.loc[
            self._stack_relation_csv[parent_col] !='0']
        Modifications = pd.DataFrame(stackrel[modname_col])
        Modifications['tmp'] = stackrel[modpre_col]
        Modifications.columns = [db.modifications.modification_name.key,
                                 db.modifications.modification_prefix.key]
        Modifications[db.modifications.modification_id.key] = \
            self._query_new_ids(db.modifications.modification_id, Modifications.shape[0])
        return Modifications

    def _generate_stackmodification(self):
        """
        generates the stackmodification table
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        key_map = {parent_col: db.stack_modifications.stack_id_parent.key,
                   modname_col: db.modifications.modification_name.key,
                   stack_col: db.stack_modifications.stack_id_child.key}

        StackModification = (self._stack_relation_csv
                             .loc[self._stack_relation_csv[parent_col] !='0',
                         list(key_map.keys())]
                    .rename(columns=key_map))

        stackdict = {n: i for n, i in
                     self.main_session.query(db.stacks.stack_name,
                                             db.stacks.stack_id)
                     .filter(db.stacks.stack_name.in_(
                         StackModification[db.stack_modifications.stack_id_parent.key].tolist()+
                         StackModification[db.stack_modifications.stack_id_child.key].tolist()))}
        StackModification[db.stack_modifications.stack_id_parent.key] = (
            StackModification[db.stack_modifications.stack_id_parent.key]
                                              .replace(stackdict))

        StackModification[db.stack_modifications.stack_id_child.key] = (
            StackModification[db.stack_modifications.stack_id_child.key]
                                              .replace(stackdict))
        modidict = {n: i for n, i in
                    (self.main_session.query(db.modifications.modification_name,
                                            db.modifications.modification_id)
                     .filter(db.modifications.modification_name.in_(
                         StackModification[db.modifications.modification_name.key])))}

        StackModification[db.modifications.modification_id.key] = (StackModification[
            db.modifications.modification_name.key]
                                                    .replace(modidict))

        return StackModification.loc[:,
                                     [db.stack_modifications.stack_id_parent.key,
                                      db.stack_modifications.stack_id_child.key,
                                      db.modifications.modification_id.key]]

    def _generate_refstack(self):
        """
        Generates the refstack table
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        key_map = {stack_col: db.ref_stacks.ref_stack_name.key}

        ref_stack =  (self._stack_relation_csv
                         .loc[self._stack_relation_csv[ref_col]=='0', list(key_map.keys())]
                         .rename(columns= key_map)
                         )
        scale_col = self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.SCALING_PREFIX]
        scale_names = [scale_col + n for n in
                       ref_stack[db.ref_stacks.ref_stack_name.key]]
        dat_img = self._images_csv.loc[:, scale_names]
        dat_img = dat_img.fillna(1)
        scales = dat_img.iloc[0,:]
        # assert that scales are equal in all images
        assert np.all(dat_img.eq(scales, axis=1))
        ref_stack[db.ref_stacks.scale.key] = scales.values
        ref_stack = ref_stack.append(pd.DataFrame({
            db.ref_stacks.ref_stack_name.key: OBJECTS_STACKNAME,
            db.ref_stacks.scale.key: 1}, index=[1]),ignore_index=True)
        # set uni id
        ref_stack[db.ref_stacks.ref_stack_id.key] = \
            self._query_new_ids(db.ref_stacks.ref_stack_id, (ref_stack.shape[0]))
        return ref_stack


    def _generate_stack(self):
        """
        Genes the DerivedStack
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        key_map = {stack_col: db.stacks.stack_name.key,
                   ref_col: db.ref_stacks.ref_stack_name.key}

        stack =  (self._stack_relation_csv
                          .loc[:, list(key_map.keys())]
                         .rename(columns= key_map)
                         )
        # Add the 'objects' stack
        stack = stack.append({db.stacks.stack_name.key: OBJECTS_STACKNAME,
                              db.ref_stacks.ref_stack_name.key: OBJECTS_STACKNAME}, ignore_index=True)

        fil = stack[db.ref_stacks.ref_stack_name.key] == '0'
        stack.loc[fil, db.ref_stacks.ref_stack_name.key] = stack.loc[fil,
                                                        db.stacks.stack_name.key]

        refstackdict = {n: i for n, i in (self.main_session
           .query(db.ref_stacks.ref_stack_name, db.ref_stacks.ref_stack_id)
           .filter(db.ref_stacks.ref_stack_name.in_(stack[db.ref_stacks.ref_stack_name.key])))}
        stack[db.ref_stacks.ref_stack_id.key] = stack[db.ref_stacks.ref_stack_name.key].replace(refstackdict)

        stack[db.stacks.stack_id.key] = \
                self._query_new_ids(db.stacks.stack_id, (stack.shape[0]))

        return stack


    def _write_refplanes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_refplanemeta()
        self._bulkinsert(planes, db.ref_planes)


    def _write_planes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_planemeta()
        self._bulkinsert(planes, db.planes)

    def _generate_refplanemeta(self):

        stack_col = self.conf[conf.STACK_DIR][conf.STACK]
        id_col = self.conf[conf.STACK_DIR][conf.ID]
        name_col = self.conf[conf.STACK_DIR][conf.NAME]
        type_col = self.conf[conf.STACK_DIR][conf.TYPE]
        planes = pd.DataFrame(

            columns=[
            db.ref_planes.ref_plane_id.key,
            db.ref_stacks.ref_stack_name.key,
            db.ref_planes.channel_name.key,
            db.ref_planes.channel_type.key
        ])
        for stack in self.stack_csvs:
            self.stack_csvs[stack].rename(columns={
                id_col:db.ref_planes.ref_plane_id.key,
                stack_col:db.ref_stacks.ref_stack_name.key,
                name_col: db.ref_planes.channel_name.key,
                type_col: db.ref_planes.channel_type.key
            }, inplace = True)
            planes = planes.append(self.stack_csvs[stack])
        planes = planes.reset_index()
        del planes['index']
        # cast PlaneID to be identical to the one in Measurement:
        planes[db.ref_planes.ref_plane_id.key] = planes[db.ref_planes.ref_plane_id.key].apply(lambda x: int(x))

        planes = planes.append({db.ref_planes.ref_plane_id.key: OBJECTS_PLANEID,
                       db.ref_stacks.ref_stack_name.key: OBJECTS_STACKNAME,
                       db.ref_planes.channel_name.key: OBJECTS_CHANNELNAME,
                       db.ref_planes.channel_type.key: OBJECTS_CHANNELTYPE},
                               ignore_index=True)
        refdict = self._get_namekey_dict(db.ref_stacks.ref_stack_name, db.ref_stacks.ref_stack_id,
                                         planes[db.ref_stacks.ref_stack_name.key].unique())

        planes[db.ref_stacks.ref_stack_id.key] = planes[db.ref_stacks.ref_stack_name.key].replace(refdict)

        return planes.loc[:, [db.ref_stacks.ref_stack_id.key, db.ref_planes.ref_plane_id.key, db.ref_planes.channel_name.key,
                              db.ref_planes.channel_type.key]]

    def _generate_planemeta(self):
        stack = self._generate_stack()
        refplanes = self._generate_refplanemeta()
        planes = stack.merge(refplanes, on=db.ref_stacks.ref_stack_id.key)
        stackdic = self._get_namekey_dict(db.stacks.stack_name, db.stacks.stack_id,
                                    planes[db.stacks.stack_name.key].unique().tolist())
        planes[db.stacks.stack_id.key] = planes[db.stacks.stack_name.key].replace(stackdic)
        planes = planes.loc[:,[db.stacks.stack_id.key,
                               db.ref_stacks.ref_stack_id.key,
                               db.ref_planes.ref_plane_id.key]]
        planes[db.planes.plane_id.key] = \
            self._query_new_ids(db.planes.plane_id, (planes.shape[0]))
        return planes

    def _write_site_table(self):
        (table,links) = self._generate_site()
        self._bulkinsert(table, db.sites)
        session = self.main_session
        for image in links.iterrows():
            img = image[1]
            session.query(db.images).\
                filter(db.images.image_id == img[db.images.image_id.key]).\
                update({db.sites.site_name.key: img[db.sites.site_name.key]})
        session.commit()

    def _generate_site(self):
        """
        generates the Site Table and a dataframe linking ImageNumber to site
        """
        names = self._generate_masks()
        rege = \
            re.compile(self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.META_REGEXP])
        names[db.sites.site_name.key] = names[db.masks.file_name.key].apply(lambda x:
                                                              rege.match(x).group(self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.GROUP_SITE]))

        links = names[[db.images.image_id.key, db.sites.site_name.key]]
        table = names[db.sites.site_name.key].drop_duplicates().to_frame()
        return table, links


    def _write_image_table(self):
        """
        Generates the Image
        table and writes it to the database.
        """
        image = self._generate_image()
        self._bulkinsert(image, db.images)

    def _generate_image(self):
        """
        Generates the Image
        table.
        """
        image = pd.DataFrame(self._images_csv[db.images.image_number.key])
        image[db.images.image_id.key] = self._query_new_ids(db.images.image_id,
                                                            image.shape[0])
        return image

    def _write_objects_table(self):
        """
        Generates and save the cell table
        """
        objects = self._generate_objects()
        self._bulkinsert(objects, db.objects)


    def _generate_objects(self):
        """
        Genertes the cell table
        """
        objects = pd.DataFrame(self._measurement_csv[[db.objects.object_number.key,
                                                      db.objects.object_type.key,
                                                      db.images.image_number.key]])


        objects[db.objects.object_id.key] = \
            self._query_new_ids(db.objects.object_id, (objects.shape[0]))
        # TODO: fix this
        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        objects[db.images.image_id.key] = objects[db.images.image_number.key].replace(img_dict)
        return objects

    def _write_measurement_table(self, minimal):
        """
        Generates the Measurement, MeasurementType and MeasurementName
        tables and writes them to the database.
        The Measurement Table can contain an extremely high ammount of rows
        and can therefore be quite slow

        """
        measurement_meta = self._generate_measurement_meta()

        self._bulkinsert(pd.DataFrame(measurement_meta[db.measurement_names.measurement_name.key]).drop_duplicates(), db.measurement_names)
        self._bulkinsert(pd.DataFrame(measurement_meta[db.measurement_types.measurement_type.key]).drop_duplicates(), db.measurement_types)
        self._bulkinsert(
            measurement_meta,
            db.measurements)

        measurements = self._generate_measurements(minimal, measurement_meta)
        # increase performance
        if self.conf[conf.BACKEND] == conf.CON_MYSQL:
            self.db_conn.execute('SET FOREIGN_KEY_CHECKS = 0')
            self.db_conn.execute('SET UNIQUE_CHECKS = 0')
            self._bulkinsert(measurements, db.object_measurements)
            self.db_conn.execute('SET FOREIGN_KEY_CHECKS = 1')
            self.db_conn.execute('SET UNIQUE_CHECKS = 1')
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_pkey;')
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_measurement_id_fkey;')
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_object_id_fkey;')
            self._bulk_pg_insert(measurements, db.object_measurements)
            self.db_conn.execute('''ALTER TABLE public.object_measurements
                                   ADD CONSTRAINT object_measurements_pkey PRIMARY KEY(object_id, measurement_id);''')
            self.db_conn.execute('''ALTER TABLE public.object_measurements
                                   ADD CONSTRAINT object_measurements_measurement_id_fkey FOREIGN KEY (measurement_id)
                                       REFERENCES public.measurements (measurement_id) MATCH SIMPLE
                                       ON UPDATE NO ACTION ON DELETE NO ACTION;''')
            self.db_conn.execute('''ALTER TABLE public.object_measurements
                                   ADD CONSTRAINT object_measurements_object_id_fkey FOREIGN KEY (object_id)
                                       REFERENCES public.objects (object_id) MATCH SIMPLE
                                       ON UPDATE NO ACTION ON DELETE NO ACTION;''')


        del self._measurement_csv

    def _generate_measurement_meta(self):
        measurements = self._measurement_csv
        measurements = measurements.drop([db.objects.object_type.key,
                                          db.images.image_number.key,
                                          db.objects.object_number.key], axis=1)
        meta = pd.Series(measurements.columns.unique()).apply(
            lambda x: lib.find_measurementmeta(self._stacks, x,
                                               no_stack_str=OBJECTS_STACKNAME,
                                              no_plane_string=OBJECTS_PLANEID))
        meta.columns = ['variable', db.measurement_types.measurement_type.key, db.measurement_names.measurement_name.key,
                        db.stacks.stack_name.key, db.ref_planes.ref_plane_id.key]
        meta = meta.loc[meta['variable'] != '', :]
        meta[db.ref_planes.ref_plane_id.key] = meta[db.ref_planes.ref_plane_id.key].map(lambda x: int(x.replace('c','')))

        dat_planeids = pd.read_sql(self.main_session.query(
                db.stacks.stack_name, db.planes.ref_plane_id, db.planes.plane_id)
            .join(db.planes).statement, self.db_conn)

        meta = meta.merge(dat_planeids)
        meta[db.measurements.measurement_id.key] = \
            self._query_new_ids(db.measurements.measurement_id,
                                (meta.shape[0]))
        return meta

    def _generate_measurements(self, minimal, meta):
        measurements = self._measurement_csv

        if minimal:
            stackrel = self._stack_relation_csv
            stackconf = self.conf[conf.STACK_RELATIONS]
            stackrel = stackrel.loc[stackrel[stackconf[conf.REF]]=='0']
            refstacks = list(stackrel[stackconf[conf.STACK]])
            meta_filt = meta.loc[(meta[db.stacks.stack_name.key].isin(refstacks)) & (meta[db.measurement_types.measurement_type.key]!="Location")]
            filtered_names = meta_filt['variable'].unique()
            filtered_names = filtered_names[filtered_names!='']
            meta = meta_filt

            measurements = measurements.set_index([db.images.image_number.key,
                     db.objects.object_number.key,'Number_Object_Number',
                    db.objects.object_type.key])
            measurements = measurements[filtered_names]
            measurements = measurements.reset_index(drop=False)

        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        measurements[db.images.image_id.key] = measurements[db.images.image_number.key].replace(img_dict)
        # Query the objects table to join the measurements with it and add the numeric,
        # per object unique index 'ObjectUniID'
        tab_obj = pd.read_sql(
            self.main_session.query(db.objects)
            .filter(db.objects.image_id.in_(measurements[db.images.image_id.key].unique().tolist()))
            .statement, self.db_conn)

        measurements = measurements.merge(tab_obj)
        measurements = measurements.drop([ db.images.image_id.key, db.images.image_number.key, db.objects.object_number.key,
                           'Number_Object_Number', db.objects.object_type.key], axis=1)
        measurements = pd.melt(measurements,
                               id_vars=[db.objects.object_id.key],
                               var_name='variable', value_name=db.object_measurements.value.key)

        # Add the MeasurementID by merging the table
        # -> currently not needed...
        #tab_meas = pd.read_sql(
        #    self.main_session.query(db.measurements.measurement_id,
        #                           db.measurements.measurement_name,
        #                           db.measurements.measurement_type,
        #                           db.planes.ref_plane_id,
        #                           db.stacks.stack_name)
        #    .join(db.planes)
        #    .join(db.stacks)
        #    .filter(db.stacks.stack_name.in_(meta[db.stacks.stack_name.key].unique()))
        #    .statement, self.db_conn)
        #meta = meta.merge(tab_meas)
        meta = meta.loc[:, ['variable', db.measurements.measurement_id.key]]
        measurements = measurements.merge(meta, how='inner', on='variable')
        measurements[db.object_measurements.value.key].replace(np.inf, 2**16, inplace=True)
        measurements[db.object_measurements.value.key].replace(-np.inf, -(2**16), inplace=True)
        measurements.dropna(inplace=True)
        measurements = measurements.loc[:,[db.objects.object_id.key,
                                                 db.measurements.measurement_id.key,
                                          db.object_measurements.value.key]]

        return measurements



    def _generate_masks(self):
        cpconf = self.conf[conf.CPOUTPUT]
        objects = cpconf[conf.MEASUREMENT_CSV][conf.OBJECTS]
        prefix = cpconf[conf.IMAGES_CSV][conf.MASKFILENAME_PEFIX]
        dat_mask = {obj:
                    self._images_csv[
                        [db.images.image_number.key, prefix+obj]
                    ].rename(columns={prefix+obj: db.masks.file_name.key})
         for obj in objects}
        dat_mask = pd.concat(dat_mask, names=[db.objects.object_type.key, 'idx'])
        dat_mask = dat_mask.reset_index(level=db.objects.object_type.key, drop=False)
        dat_mask = dat_mask.reset_index(drop=True)
        mask_regexp = cpconf[conf.IMAGES_CSV][conf.META_REGEXP]
        if mask_regexp is not None:
            """
            Try to get the crop information from the provided regexp
            """
            (dat_mask[db.masks.crop_number.key], dat_mask[db.masks.pos_x.key],
            dat_mask[db.masks.pos_y.key], dat_mask[db.masks.shape_w.key], dat_mask[db.masks.shape_h.key]) = \
            zip(*dat_mask[db.masks.file_name.key].map(lambda x:
                                               [re.match(mask_regexp, x).groupdict()
                                               .get(col, None) for col in
                                                [db.masks.crop_number.key, db.masks.pos_x.key,
                                                 db.masks.pos_y.key, db.masks.shape_w.key,
                                                 db.masks.shape_h.key]]))

            if all(dat_mask[db.masks.shape_w.key].isnull()):
                """
                If the width and height are not in the regexp, load all the
                mask and check the width
                """
                cpconf = self.conf[conf.CPOUTPUT]
                basedir = cpconf[conf.IMAGES_CSV][conf.MASK_DIR]
                if basedir is None:
                    basedir = self.conf[conf.CP_DIR]
                dat_mask[db.masks.shape_w.key], dat_mask[db.masks.shape_h.key] = \
                        zip(*dat_mask[db.masks.file_name.key].map(lambda fn:
                                tif.imread(os.path.join(basedir, fn)).shape))
        else:
            dat_mask[db.masks.crop_number.key] = None
            dat_mask[db.masks.pos_x.key] = 0
            dat_mask[db.masks.pos_y.key] = 0
            dat_mask[db.masks.shape_h.key] = None
            dat_mask[db.masks.shape_w.key] = None
        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        dat_mask[db.masks.image_id.key] = dat_mask[db.images.image_number.key].replace(img_dict)
        return dat_mask

    def _write_masks_table(self):
        masks = self._generate_masks()
        self._bulkinsert(masks, db.masks)

    def _generate_object_relation_types(self):
        dat_relations = (self._relation_csv)
        dat_types =  pd.DataFrame(dat_relations.loc[:,
                db.object_relation_types.object_relationtype_name.key]).drop_duplicates()
        dat_types[db.object_relation_types.object_relationtype_id.key] = \
            self._query_new_ids(db.object_relationstype.object_relationtype_id, (dat_types.shape[0]))
        return dat_types

    def _generate_object_relations(self):
        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        relation_dict = {n: i for n, i in
                         self.main_session.query(db.object_relation_types.object_relationtype_name,
                                                 db.object_relation_types.object_relationtype_id)}
        obj_dict = {(imgid, objnr, objtype): objid
                    for imgid, objnr, objtype, objid in
                    self.main_session.query(db.objects.image_id,
                                            db.objects.object_number,
                                            db.objects.object_type,
                                            db.objects.object_id)}

        dat_relations = (self._relation_csv)
        dat_relations['timg'] = dat_relations[conf.IMAGENUMBER_FROM].replace(img_dict)
        dat_relations[db.object_relations.object_id_parent.key] =\
            dat_relations.loc[:,['timg',
                                 conf.OBJECTNUMBER_FROM,
                                 conf.OBJECTTYPE_FROM]].apply(
                lambda x: obj_dict.get((x[0], x[1], x[2])), axis=1)
        dat_relations['timg'] = dat_relations[conf.IMAGENUMBER_TO].replace(img_dict)
        dat_relations[db.object_relations.object_id_child.key] =\
            dat_relations.loc[:,['timg',
                                 conf.OBJECTNUMBER_TO,
                                 conf.OBJECTTYPE_TO]].apply(
                lambda x: obj_dict.get((x[0], x[1], x[2])), axis=1)
        dat_relations[db.object_relations.object_relationtype_id.key] = \
            dat_relations[db.object_relation_types.object_relationtype_name.key].replace(relation_dict)
        return dat_relations

    def _write_object_relations_table(self):
        relation_types = self._generate_object_relation_types()
        self._bulkinsert(relation_types, db.object_relation_types)
        relations = self._generate_object_relations()
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            self._bulk_pg_insert(relations, db.object_relations)
        else:
            self._bulkinsert(relations, db.object_relations)

    def _write_pannel_table(self):
        pannel = self._generate_pannel_table()
        self._bulkinsert(pannel, db.pannel)

    def _generate_pannel_table(self):
        csv_pannel = self.pannel
        conf_pannel = self.conf[conf.PANNEL_CSV]
        col_map = {conf_pannel[c]: target for c, target in [
            (conf.PANEL_CSV_CHANNEL_NAME, db.pannel.metal.key),
            (conf.PANEL_CSV_ILASTIK_NAME, db.pannel.is_ilastik.key),
            (conf.PANEL_CSV_BARCODE_NAME, db.pannel.is_barcode.key),
            (conf.PANEL_CSV_CLONE_NAME, db.pannel.antibody_clone.key),
            (conf.PANEL_CSV_CONCENTRATION_NAME, db.pannel.concentration.key),
            (conf.PANEL_CSV_TARGET_NAME, db.pannel.target.key),
            (conf.PANEL_CSV_TUBE_NAME, db.pannel.tube_number.key)]}
        cols = [c for c in col_map]
        csv_pannel.drop(list(set(csv_pannel.columns) - set(cols)), axis=1, inplace=True)
        csv_pannel = csv_pannel.rename(columns=col_map)
        #correct conc to Float
        csv_pannel[db.pannel.concentration.key] = csv_pannel[db.pannel.concentration.key].apply(
            lambda x: float(re.findall(r"[-+]?\d*\.\d+|\d+", x)[0])
        )
        # correct boolean to logical
        csv_pannel.loc[:, [db.pannel.is_barcode.key, db.pannel.is_ilastik.key]] =\
                csv_pannel.loc[:, [db.pannel.is_barcode.key, db.pannel.is_ilastik.key]] == 1
        return csv_pannel

    def _write_condition_table(self):
        conditions = self._generate_condition_table()
        if conditions is not None:
            self._bulkinsert(conditions, db.conditions)


    def _generate_condition_table(self):
        rename_dict = {self.conf[conf.LAYOUT_CSV][c]: target for c, target in [
                (conf.LAYOUT_CSV_COND_ID, db.conditions.condition_id.key),
                (conf.LAYOUT_CSV_COND_NAME, db.conditions.condition_name.key),
                (conf.LAYOUT_CSV_TIMEPOINT_NAME, db.conditions.time_point.key),
                (conf.LAYOUT_CSV_BARCODE, db.conditions.barcode.key),
                (conf.LAYOUT_CSV_CONCENTRATION_NAME, db.conditions.concentration.key),
                (conf.LAYOUT_CSV_BC_PLATE_NAME, db.conditions.bc_plate.key),
                (conf.LAYOUT_CSV_PLATE_NAME, db.conditions.plate_id.key),
                (conf.LAYOUT_CSV_BCX, db.conditions.bc_x.key),
                (conf.LAYOUT_CSV_BCY, db.conditions.bc_y.key)
            ] if target is not None
        }
        if rename_dict.get(None) is not None:
            del rename_dict[None]
        cols = [c for c in rename_dict]
        outcols = [rename_dict[c] for c in rename_dict]
        if self.barcode_key is not None:
            if self.experiment_layout is not None:
                barcodes = self.barcode_key.transpose().apply(lambda x: str(x.to_dict()))
                barcodes = barcodes.to_frame()
                barcodes.columns = [db.conditions.barcode.key]
                #IDs = self.barcode_key.transpose().apply(lambda x: ''.join(x.astype(str).tolist()))
                barcodes[db.conditions.condition_id.key] = \
                    self._query_new_ids(db.conditions.condition_id, (barcodes.shape[0]))
                barcodes = barcodes.reset_index(drop=False)
                barcodes = barcodes.rename(columns={self.conf[conf.BARCODE_CSV][conf.BC_CSV_PLATE_NAME]:
                                 self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_BC_PLATE_NAME]})
                data = barcodes.merge(
                    self.experiment_layout.reset_index(drop=False),
                    left_on=(self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_BC_PLATE_NAME],
                             self.conf[conf.BARCODE_CSV][conf.BC_CSV_WELL_NAME]),
                    right_on=(self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_BC_PLATE_NAME],
                              self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_WELL_NAME]),
                    how='left'
                )
                data = data.dropna()
                tp_name = self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_TIMEPOINT_NAME]
                tw_name = self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_WELL_NAME]
                we_name = self.conf[conf.BARCODE_CSV][conf.BC_CSV_WELL_NAME]
                co_name = self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_COND_NAME]
                data.loc[pd.isnull(data[tw_name]),co_name] = "default"
                data.loc[pd.isnull(data[tw_name]),tp_name] = 0.0
                data.loc[pd.isnull(data[tw_name]),tw_name] = data.loc[pd.isnull(data[tw_name])][we_name]
                data[db.conditions.bc_y.key] = data[self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_WELL_NAME]].apply(lambda x: x[0])
                data[db.conditions.bc_x.key] = data[self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_WELL_NAME]].apply(lambda x: int(x[1:]))

                data = data.rename(columns=rename_dict)
                if self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_TIMEPOINT_NAME] is None:
                    data[db.conditions.time_point.key] = 0.0
                if self.conf[conf.LAYOUT_CSV][conf.LAYOUT_CSV_COND_NAME] is None:
                    data[db.conditions.condition_name.key] = 'default'

                data = data.fillna('0')
                return data
            else:
                barcodes = self.barcode_key.transpose().apply(lambda x: str(x.to_dict()))
                barcodes = barcodes.to_frame()
                barcodes.columns = [db.conditions.barcode.key]
                IDs = self.barcode_key.transpose().apply(lambda x: ''.join(x.astype(str).tolist()))
                barcodes[db.conditions.condition_id.key] = IDs
                barcodes = barcodes.reset_index(drop=False)

                barcodes = lib.fill_null(barcodes, db.conditions)
                barcodes[db.conditions.bc_y.key] = barcodes[self.conf[conf.BARCODE_CSV][conf.BC_CSV_WELL_NAME]].apply(lambda x: x[0])
                barcodes[db.conditions.bc_x.key] = barcodes[self.conf[conf.BARCODE_CSV][conf.BC_CSV_WELL_NAME]].apply(lambda x: int(x[1:]))
                barcodes[db.conditions.bc_plate.key] = barcodes[self.conf[conf.BARCODE_CSV][conf.BC_CSV_PLATE_NAME]]
                return barcodes[outcols]
        else:
            return None




    def _query_new_ids(self, id_col, n):
        """
        Queries non used id's from the database
        Args:
            id_col: a sqlalchemy column object corresponding
                to a column in a table
            n: how many id's are requested
        """
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            session = self.session_maker()
            str_seq = str(id_col).replace('.', '_')+'_seq'
            session.execute('ALTER SEQUENCE ' + str_seq + ' INCREMENT ' + str(n))
            i = session.execute(sa.schema.Sequence(str_seq))
            session.execute('ALTER SEQUENCE ' + str_seq + ' INCREMENT ' + str(1))
            session.commit()
            return range(i-n+1, i+1)
        else:
            prev_max = self.db_conn.query(sa.func.max(id_col)).scalar()
            return range(prev_max+1, prev_max+n+1)





    #########################################################################
    #########################################################################
    #                           setter functions:                           #
    #########################################################################
    #########################################################################

    def _bulkinsert(self, data, table, drop=None):
        """_bulkinsert
        This function is used for Bulk inserting data to the database.
        Note that dropping all entries in a table can fail because of
        foregn key constraints. It is recommended to only use this method
        at the first data import.

        Args:
            DataFrame data: the data to be inserted
            sqlalchemy table: the target table
            bool drop: if the table should be emptied before inserting.
                default False.
        """
        if drop is None:
            drop = False

        dbtable = str(self.db_conn.url)+'::'+table.__table__.name
        if drop:
            session = self.main_session
            session.query(table).delete()
            session.commit()

        print('Insert table of dimension:', str(data.shape))
        data = self._clean_columns(data, table)

        odo(data, dbtable)
        self.main_session.commit()

    def _bulk_pg_insert(self, data, table, drop=False):
        if drop:
            session = self.main_session
            session.query(table).delete()
            session.commit()
        print('Insert table of dimension:', str(data.shape))
        data = self._clean_columns(data, table)
        output = io.StringIO()
        # ignore the index
        data.to_csv(output, sep='\t', header=False, index=False)
        output.getvalue()
        # jump to start of stream
        output.seek(0)
        con = self.db_conn
        connection = con.raw_connection()
        cursor = connection.cursor()
        # null values become ''
        table_name = table.__tablename__
        cursor.copy_from(output, table_name, null="")
        connection.commit()
        cursor.close()

    def _clean_columns(self, data, table):
        data_cols = data.columns
        table_cols = table.__table__.columns.keys()
        uniq = list(set(table_cols)-set(data_cols))
        data = data.loc[:, table_cols]
        for un in uniq:
            data[un] = None
        return data

    def add_measurements(self, measurements, replace=False, backup=False,
        col_image = db.images.image_id.key,
        col_object_no = db.objects.object_number.key,
        col_object_id = db.objects.object_id.key,
        col_type = db.measurement_types.measurement_type.key,
        col_name = db.measurement_names.measurement_name.key,
        col_plane = db.ref_planes.ref_plane_id.key,
        col_stackname = db.stacks.stack_name.key,
        col_value = db.object_measurements.value.key,
        split = 100000
    ):
        """add_measurements
        This function allows to store new measurements to the database.
        If overwrite == False, it will only add new measurements, discard the
        ones where a key already exist and warn you about any dropped
        measurements.
        If overwrite == True, it will overwrite existing measurements. use
        with care!

        Args:
            Pandas.DataFrame measurements: the measurements to be written.
            bool replace: should existing measurements be updated?
        Returns:
            Pandas.DataFrame containing the deleted tuples. These can be used
                to restore the old ones.
            Pandas.DataFrame containing the unstored rows
        """
        col_map = {c: target for c, target in [
            (col_image, db.images.image_id.key),
            (col_object_no, db.objects.object_number.key),
            (col_object_id, db.objects.object_id.key),
            (col_type, db.measurement_types.measurement_type.key),
            (col_name, db.measurement_names.measurement_name.key),
            (col_plane, db.ref_planes.ref_plane_id.key),
            (col_stackname, db.stacks.stack_name.key),
            (col_value, db.object_measurements.value.key)]}

        measurements_base = measurements.rename(columns=col_map)
        finished = False
        bak_t = un_t = measurements_base[0:0]
        print("starting storing measurements...")
        while not finished:
            print("still need to store "+str(len(measurements_base))+" tuples!")
            if len(measurements_base) > split:
                measurements = measurements_base[:split]
                measurements_base = measurements_base[split:]
            else:
                measurements = measurements_base
                finished = True

            images = [int(c) for c in measurements[db.images.image_id.key].unique()]
            objects = [int(c) for c in measurements[db.objects.object_number.key].unique()]
            object_id = [str(c) for c in measurements[db.objects.object_id.key].unique()]
            measurement_type = [str(c) for c in measurements[db.measurement_types.measurement_type.key].unique()]
            measurement_name = [str(c) for c in measurements[db.measurement_names.measurement_name.key].unique()]
            plane = [str(c) for c in measurements[db.ref_planes.ref_plane_id.key].unique()]
            stack = [str(c) for c in measurements[db.stacks.stack_name.key].unique()]

            query =  self.main_session.query(db.object_measurements).filter(
                db.object_measurements.ImageNumber.in_(images),
                db.object_measurements.ObjectNumber.in_(objects),
                db.object_measurements.ObjectID.in_(object_id),
                db.object_measurements.MeasurementType.in_(measurement_type),
                db.object_measurements.MeasurementName.in_(measurement_name),
                db.object_measurements.PlaneID.in_(plane),
                db.object_measurements.StackName.in_(stack)
            )

            (bak, un) =  self._add_generic_tuple(measurements, db.object_measurements,query=query, replace=replace, backup=backup)
            bak_t.append(bak)
            un_t.append(un)
        return (bak_t, un_t)


    def _add_generic_tuple(self, data, table, query=None, replace=False, backup=False):
        """add_generic_tuple
        adds tuples from date to the database and returns non stored or
        deleted values.

        Args:
            Pandas DataFrame data: dataframe containing the data. It is
                required to name the columns according to the db schema
            Sqlalchemy Table table: the table object to be added to
            sqlalchemy query query: query object to retrieve existing tuples.
                best option: query for all keys! If no query is supplied,
                a query will be generated based on the table keys
            bool replace: if existing tuples should be replaced
            backup: only used if replace = True. Specifies whether a table
                with the deleted tuples should be returned. Can speed up
                operation

        Returns:
            Pandas.DataFrame containing the deleted tuples. These can be used
                to restore the old ones.
            Pandas.DataFrame containing the unstored rows

        """
        data = data.reset_index(drop=True)
        key_cols = [key.name for key in inspect(table).primary_key]
        if query is None:
            query = self.main_session.query(table)
            for key in key_cols:
                filt_in = data[key].astype(str).unique()
                query = query.filter(table.__table__.columns[key].in_(filt_in))
        if replace:
            if backup:
                backup =  pd.read_sql(query.statement, self.db_conn)
            else:
                backup = None

            query.delete(synchronize_session='fetch')
            self.main_session.commit()
            self._bulkinsert(data, table)

            return backup, None
        else:
            backup =  pd.read_sql(query.statement, self.db_conn)
            current = backup.copy()
            zw = data[key_cols].append(current[key_cols]).drop_duplicates(keep=False)
            storable = data.merge(zw)

            lm, ls = len(data), len(storable)
            if lm != ls:
                miss = lm - ls
                stri = 'There were '
                stri += str(miss)
                stri += ' rows that were not updated in '
                stri += table.__tablename__
                stri += '! This does not mean that something went wrong, but '
                stri += 'maybe you tried to readd some rows.'
                warnings.warn(stri, UserWarning)

            self._bulkinsert(storable, table)

            unstored = data.merge(zw, how='outer')

            return None, unstored


    #########################################################################
    #########################################################################
    #                           getter functions:                           #
    #########################################################################
    #########################################################################
    def get_panel(self):
        """get_panel
        convenience method to get the full Panel
        """
        session = self.main_session
        result = pd.read_sql(session.query(db.pannel).statement,self.db_conn)
        return  result

    def get_metal_from_name(self, name):
        """get_metal_from_name
        Returns a tuple (metal, info) where info is the corresponding row in
        in the Panel, containing additional info.

        Args:
            str name: the name of the target
        Returns:
            str metal: The metal name corresponding to the name or
                name if no metal was found
            Pandas Dataframe info: a Dataframe containing aditional info about the metal.
                None if no metal was found.
        """

        session = self.main_session
        result = pd.read_sql(session.query(db.pannel).filter(db.pannel.target==name).statement,self.db_conn)
        if len(result) > 0:
            return (result[db.pannel.metal.key], result)
        else:
            return (name, None)

    def get_name_from_metal(self, metal):
        """get_name_from_metal
        Returns a tuple (name, info) where info is the corresponding row in
        in the Panel, containing additional info.

        Args:
            str metal: the name of the target
        Returns:
            str name: The target name corresponding to the metal or
                metal if no metal was found
            Pandas Dataframe info: a Dataframe containing aditional info about the Target.
                None if no target was found.
        """

        session = self.main_session
        result = pd.read_sql(session.query(db.pannel).filter(db.pannel.metal==metal).statement,self.db_conn)
        if len(result) > 0:
            return (result[db.pannel.target.key], result)
        else:
            return (metal, None)

    def get_image_meta(self,
        image_number = None):
        """get_measurement_types
        Returns a pandas DataFrame containing image information.
        Integers or strings lead to a normal WHERE clause:
        ...
        WHERE ImageNumber = 1 AND
        ...
        If you specify an array as a filter, the WHERE clause in the query will
        look like this:
        ...
        WHERE ImageNumber IN (1,2,3,4) AND
        ...
        If you dont specify a value, the WHERE clause will be omitted.

        Args:
            int/array image_number: ImageNumber. If 'None', do not filter

        Returns:
            DataFrame
        """

        args = locals()
        args.pop('self')
        return self.get_table_data(db.TABLE_IMAGE,  **args)

    def get_cell_meta(self,
        image_number = None,
        object_number = None,
                     object_id = None):
        """get_measurement_types
        Returns a pandas DataFrame containing image information.
        Integers or strings lead to a normal WHERE clause:
        ...
        WHERE ImageNumber = 1 AND
        ...
        If you specify an array as a filter, the WHERE clause in the query will
        look like this:
        ...
        WHERE ImageNumber IN (1,2,3,4) AND
        ...
        If you dont specify a value, the WHERE clause will be omitted.

        Args:
            int/array image_number: ImageNumber. If 'False', do not filter
            int/array object_number: CellNumber. If 'False', do not filter

        Returns:
            DataFrame
        """

        args = locals()
        args.pop('self')
        return self.get_table_data(db.objects.__tablename__,  **args)

    def get_stack_meta(self,
        stack_name = None):
        """get_stack_meta
        Returns a pandas DataFrame containing image information.
        Integers or strings lead to a normal WHERE clause:
        ...
        WHERE ImageNumber = 1 AND
        ...
        If you specify an array as a filter, the WHERE clause in the query will
        look like this:
        ...
        WHERE ImageNumber IN (1,2,3,4) AND
        ...
        If you dont specify a value, the WHERE clause will be omitted.

        Args:
            int/array stack_name: ImageNumber. If 'False', do not filter

        Returns:
            DataFrame
        """
        # TODO: This function had an obvious bug before. However the query
        # still does not work. Please explain.
        query = 'SELECT Stack.*, DerivedStack.RefStackName FROM Stack'

        if stack_name is None:
            clause_dict = None
        else:
            clause_dict = {db.stacks.stack_name.key: stack_name}

        #stack_name
        self._sqlgenerate_simple_query('Stack', columns=['Stack.*',
                                                'DerivedStack.RefStackName'],
                                       clause_dict=clause_dict)
        query += ' LEFT JOIN DerivedStack ON Stack.StackName = DerivedStack.RefStackName'
        return pd.read_sql_query(query, con=self.db_conn)


    def get_measurement_meta(self, cached = True):
        """get_measurement_types
        Returns a pandas DataFrame containing Measurement information.
        Slow, it is recommended to use the cached value

        Args:
            cached: If True, then use the cached Value. If False, always execute
                query.

        Returns:
            DataFrame containing:
            MeasurementName | MeasurementType | StackName
        """
        query = "select distinct MeasurementName, MeasurementType, StackName From Measurement;"
        if (cached and self.measurement_meta_cache is not None):
            return self.measurement_meta_cache
        else:
            self.measurement_meta_cache = pd.read_sql_query(query, con=self.db_conn)
            return self.measurement_meta_cache

    def get_measurements(self,
        image_number=None,
        object_number=None,
        oject_id=None,
        measurement_type=None,
        measurement_name=None,
        stack_name=None,
        plane_id=None,
        columns=None
        ):
        """get_measurement_types
        Returns a pandas DataFrame containing Measurements according to the
        specified filters.
        Integers or strings lead to a normal WHERE clause:
        ...
        WHERE ImageNumber = 1 AND
        ...
        If you specify an array as a filter, the WHERE clause in the query will
        look like this:
        ...
        WHERE ImageNumber IN (1,2,3,4) AND
        ...
        If you dont specify a value, the WHERE clause will be omitted.

        Args:
            int/array image_number: ImageNumber. If NONE, do not filter
            int/array object_number: CellNumber. If NONE, do not filter
            str/array measurement_type: MeasurementType. If NONE, do not filter
            str/array measurement_name: MeasurementName. If NONE, do not filter
            str/array stack_name: StackName. If NONE, do not filter
            str/array plane_id: PlaneID. If NONE, do not filter

        Returns:
            DataFrame containing:
            MeasurementName | MeasurementType | StackName
        """

        args = locals()
        args.pop('self')
        return self.get_table_data(db.object_measurements.__tablename__,  **args)

    def get_(self, arg):
        pass

    def get_table_data(self, table, columns=None, clause_dict=None, connection=None, **kwargs):
        """
        General wrapper that allows the retrieval of data  from the database

        Allows to generate queries from the format
        Select COLUMNS from TABLE WHERE COLUMN1 in VALUES1 AND COLUMN2 in
        VALUES2

        Args:
            table: the name of the table
            columns: the columns name, default all ('*')
            clause_dict: A dict of the form:
                {COLUMN_NAME1: LIST_OF_VALUES1, COLUMN_NAME2: ...}
            connection: the Database connection, default: self.db_conn
            **kwargs: The kwargs will be searched for  arguments with names
            contained in 'DICT_DB_KEYS' - valid registed names. The clause_dict
            will be updated with these additional entries.

        Returns:
            The queried table.
        """
        query = self._sqlgenerate_simple_query(table, columns=columns,
                                       clause_dict=clause_dict,
                                       **kwargs)

        if connection is None:
            connection = self.db_conn

        return pd.read_sql_query(query, con=connection)

    # def get_plane_ids(channel_names, stack_names):
        # """
        # Retreive the plane id from channel names and stack.
        # """
        # 'SELECT * FROM {} INNER JOIN '.format(db.TABLE_PLANES,db.

    def _sqlgenerate_simple_query(self, table, columns=None, clause_dict=None,
                                  connection=None, **kwargs):
        """
        Helper function to generate simple queries
        Consult helf from "get_table_data" for details
        """
        if clause_dict is None:
            clause_dict = {}

        key_dict = lib.filter_and_rename_dict(kwargs, DICT_DB_KEYS)
        clause_dict.update(key_dict)
        clauses = lib.construct_in_clause_list(clause_dict)
        query = lib.construct_sql_query(table, columns=columns, clauses=clauses)
        return query

    def get_measurement_query(self, session=None):
        """
        Returns a query object that queries table with the most important
        information do identify a measurement
        """
        if session is None:
            session = self.main_session
        query = (session.query(db.ref_planes.channel_name,
                                    db.ref_planes.channel_type,
                                    db.images.image_id,
                                   db.objects.object_number,
                                   db.objects.object_type,
                                   db.object_measurements.MeasurementName,
                                   db.object_measurements.MeasurementType,
                                   db.object_measurements.value,
                                   db.object_measurements.PlaneID)
            .join(db.planes)
            .join(db.object_measurements)
            .join(db.objects)
            .join(db.images)
                )
        return query


    def _get_table_object(self, name):
        return getattr(db, name)

    def _get_column_from_table(self, table_obj, col_name):
        return getattr(table_obj, col_name)

    def _get_table_column(self, table_name, col_name):
        tab = self._get_table_object(table_name)
        col = self._get_column_from_table(tab, col_name)
        return col

    def _get_table_columnnames(self, table_name):
        tab = self._get_table_object(table_name)
        return tab.__table__.columns.keys()

    def _get_table_keynames(self, table_name):
        tab = self._get_table_object(table_name)
        return tab.__table__.primary_key.column.keys()

    def _get_namekey_dict(self, namecol, idcol, names):
        """
        Generates a name: idcol dictionary from a table
        while filtering for names in the namecol

        namecol: A sql column, e.g. db.Stack.StackName
        idcol: A sql id column, e.g. db.Stack.StackID
        names: names to be queried

        """
        d = {n: i for n, i in (self.main_session.query(namecol, idcol)
                               .filter(namecol.in_(names)))}
        return d

    def _pg_vacuum(self):
        self.db_conn.execution_options(isolation_level="AUTOCOMMIT").execute('VACUUM ANALYZE;')

    #Properties:
    @property
    def pannel(self):
        if self._pannel is None:
            self._read_pannel()
        return self._pannel

    @property
    def _name_dict(self):
        conf_pannel = self.conf[conf.PANNEL_CSV]
        col_channel =  conf_pannel[conf.CHANNEL_NAME]
        col_name = conf_pannel[conf.DISPLAY_NAME]
        name_dict = {metal: name for metal, name in zip(
            self._pannel[col_channel], self._pannel[col_name]
        )}
        return name_dict

    @property
    def _stacks(self):
        stacks = list(
            self._stack_relation_csv[self.conf[conf.STACK_RELATIONS][conf.STACK]])
        stacks += [s for s in [st for st in self.stack_csvs]]
        return set(stacks)

    @property
    def session_maker(self):
        """
        Returns the session maker object for the current database connection
        """
        if self._session_maker is None:
            self._session_maker = sessionmaker(bind=self.db_conn)
        return self._session_maker

    @property
    def main_session(self):
        """
        Returns the current database main session
        to query the database in an orm way.
        """
        if self._session is None:
            self._session = self.session_maker()
        return self._session

    def get_query_function(self):
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            from psycopg2 import connect
            from sqlalchemy.dialects import postgresql
            connection = connect(
                        host=self.conf[conf.CON_POSTGRESQL]['host'],
                        dbname = self.conf[conf.CON_POSTGRESQL]['db'],
                        user = self.conf[conf.CON_POSTGRESQL]['user'],
                        password = self.conf[conf.CON_POSTGRESQL]['pass']
            )
            def query_postgres(query):
                comp = query.statement.compile(dialect=postgresql.dialect())
                d = pd.read_sql(comp.string, connection,params=comp.params)
                return d
            return query_postgres
        else:
            def query_general(query):
                d = pd.read_sql(query.statement, self.db_conn)
                return d
