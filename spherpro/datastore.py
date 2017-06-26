import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import os
import re
import warnings

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.configuration as conf
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

DICT_DB_KEYS = {
    'image_number': db.KEY_IMAGENUMBER,
    'object_number': db.KEY_OBJECTNUMBER,
    'measurement_type': db.KEY_MEASUREMENTTYPE,
    'measurement_name': db.KEY_MEASUREMENTNAME,
    'stack_name': db.KEY_STACKNAME,
    'plane_id': db.KEY_PLANEID,
    'object_id': db.KEY_OBJECTID
}

OBJECTS_STACKNAME = 'ObjectStack'
OBJECTS_CHANNELNAME = 'object'
OBJECTS_PLANEID = 'c1'
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
            conf.CON_MYSQL: db.connect_mysql
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

    def import_data(self):
        """read_data
        Reads the Data using the file locations given in the configfile.
        """
        # Read the data based on the config
        self._read_experiment_layout()
        self._read_barcode_key()
        # self._readWellMeasurements()
        # self._read_cut_meta()
        # self._read_roi_meta()
        self._read_measurement_data()
        self._read_stack_meta()
        self._populate_db()

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
        self._read_measurement_data()
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
        sep = self.conf[conf.LAYOUT_CSV][conf.SEP]
        self.experiment_layout = pd.read_csv(
            self.conf[conf.LAYOUT_CSV][conf.PATH], sep=sep
        ).set_index(
            [self.conf[conf.LAYOUT_CSV][conf.PLATE],
             self.conf[conf.LAYOUT_CSV][conf.CONDITION]]
        )

    def _read_barcode_key(self):
        """
        reads the barcode key as stated in the config
        and saves it in the datastore
        """
        sep = self.conf[conf.BARCODE_CSV][conf.SEP]
        self.barcode_key = pd.read_csv(
            self.conf[conf.BARCODE_CSV][conf.PATH], sep=sep
        ).set_index(
            self.conf[conf.BARCODE_CSV][conf.WELL_COL]
        )

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
        cellmeas = pd.concat(cellmeas, names=[db.KEY_OBJECTID, 'idx'] )
        cellmeas = cellmeas.reset_index(level=db.KEY_OBJECTID, drop=False)
        self._measurement_csv = cellmeas
        self._images_csv = lib.read_csv_from_config(
            self.conf[conf.CPOUTPUT][conf.IMAGES_CSV],
            base_dir=cpdir)
        self._relation_csv = lib.read_csv_from_config(
            self.conf[conf.CPOUTPUT][conf.RELATION_CSV],
            base_dir=cpdir)


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


    def _populate_db(self):
        """
        writes the tables to the database
        """
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)
        db.initialize_database(self.db_conn)
        print('masks')
        self._write_masks_table()
        print('image')
        self._write_image_table()
        self._write_objects_table()
        self._write_stack_tables()
        self._write_refplanes_table()
        self._write_planes_table()
        self._write_measurement_table()
        self._write_object_relations_table()
        self._write_pannel_table()

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
        mod_query = self.main_session.query(db.Modification).filter(
            db.Modification.ModificationName.in_(modifications[db.KEY_MODIFICATIONNAME].astype(str).unique())
        )

        self._add_generic_tuple(modifications, mod_query, db.Modification)

        # RefStacks
        RefStack = self._generate_refstack()
        refst_query = self.main_session.query(db.RefStack).filter(
            db.RefStack.RefStackName.in_(RefStack[db.KEY_REFSTACKNAME].astype(str).unique())
        )
        self._add_generic_tuple(RefStack, refst_query, db.RefStack)

        # Stacks
        Stack = self._generate_stack()
        st_query = self.main_session.query(db.Stack).filter(
            db.Stack.StackName.in_(Stack[db.KEY_STACKNAME].astype(str).unique())
        )
        self._add_generic_tuple(Stack, st_query, db.Stack)

        # StackModifications
        stackmodification = self._generate_stackmodification()
        stmod_query = self.main_session.query(db.StackModification).filter(
            db.StackModification.ModificationName.in_(modifications[db.KEY_MODIFICATIONNAME].astype(str).unique()),
            db.StackModification.ParentName.in_(stackmodification[db.KEY_PARENTNAME].astype(str).unique()),
            db.StackModification.ChildName.in_(stackmodification[db.KEY_CHILDNAME].astype(str).unique())
        )
        self._add_generic_tuple(stackmodification, stmod_query, db.StackModification)




    def _generate_modifications(self):
        """
        Generates the modification table
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        modpre_col = self.conf[conf.STACK_RELATIONS][conf.MODPRE]
        stackrel = self._stack_relation_csv.loc[self._stack_relation_csv[parent_col]!='0']
        Modifications = pd.DataFrame(stackrel[modname_col])
        Modifications['tmp'] = stackrel[modpre_col]
        Modifications.columns = ['ModificationName','ModificationPrefix']
        return Modifications

    def _generate_stackmodification(self):
        """
        generates the stackmodification table
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        modpre_col = self.conf[conf.STACK_RELATIONS][conf.MODPRE]
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        key_map = {parent_col: db.KEY_PARENTNAME,
                   modname_col: db.KEY_MODIFICATIONNAME,
                   stack_col: db.KEY_CHILDNAME}
        StackModification = (self._stack_relation_csv
                    .loc[self._stack_relation_csv[parent_col]!='0',
                         list(key_map.keys())]
                    .rename(columns=key_map))
        return StackModification

    def _generate_refstack(self):
        """
        Generates the refstack table
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        key_map = {stack_col: db.KEY_REFSTACKNAME}

        ref_stack =  (self._stack_relation_csv
                         .loc[self._stack_relation_csv[ref_col]=='0', list(key_map.keys())]
                         .rename(columns= key_map)
                         )
        scale_col = self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.SCALING_PREFIX]
        scale_names = [scale_col + n for n in
                       ref_stack[db.KEY_REFSTACKNAME]]
        dat_img = self._images_csv.loc[:, scale_names]
        dat_img = dat_img.fillna(1)
        scales = dat_img.iloc[0,:]
        # assert that scales are equal in all images
        assert np.all(dat_img.eq(scales, axis=1))
        ref_stack[db.KEY_SCALE] = scales.values
        ref_stack = ref_stack.append(pd.DataFrame({
            db.KEY_REFSTACKNAME: OBJECTS_STACKNAME,
            db.KEY_SCALE: 1}, index=[1]),ignore_index=True)
        return ref_stack


    def _generate_stack(self):
        """
        Genes the DerivedStack
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        key_map = {stack_col: db.KEY_STACKNAME,
                   ref_col: db.KEY_REFSTACKNAME}

        stack =  (self._stack_relation_csv
                          .loc[:, list(key_map.keys())]
                         .rename(columns= key_map)
                         )
        # Add the 'objects' stack
        stack = stack.append({db.KEY_STACKNAME: OBJECTS_STACKNAME,
                              db.KEY_REFSTACKNAME: OBJECTS_STACKNAME}, ignore_index=True)

        fil = stack[db.KEY_REFSTACKNAME] == '0'
        stack.loc[fil, db.KEY_REFSTACKNAME] = stack.loc[fil,
                                                                        db.KEY_STACKNAME]
        return stack


    def _write_refplanes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_refplanemeta()
        query = self.main_session.query(db.RefPlaneMeta).filter(
            db.RefPlaneMeta.RefStackName.in_(planes[db.KEY_REFSTACKNAME].astype(str).unique()),
            db.RefPlaneMeta.PlaneID.in_(planes[db.KEY_PLANEID].astype(str).unique())
        )
        self._add_generic_tuple(planes, query, db.RefPlaneMeta)


    def _write_planes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_planemeta()
        query = self.main_session.query(db.PlaneMeta).filter(
            db.PlaneMeta.StackName.in_(planes[db.KEY_STACKNAME].astype(str).unique()),
            db.PlaneMeta.PlaneID.in_(planes[db.KEY_PLANEID].astype(str).unique())
        )
        self._add_generic_tuple(planes, query, db.PlaneMeta)

    def _generate_refplanemeta(self):

        stack_col = self.conf[conf.STACK_DIR][conf.STACK]
        id_col = self.conf[conf.STACK_DIR][conf.ID]
        name_col = self.conf[conf.STACK_DIR][conf.NAME]
        type_col = self.conf[conf.STACK_DIR][conf.TYPE]
        planes = pd.DataFrame(

            columns=[
            db.KEY_PLANEID,
            db.KEY_REFSTACKNAME,
            db.KEY_CHANNEL_NAME,
            db.KEY_CHANNEL_TYPE
        ])
        for stack in self.stack_csvs:
            self.stack_csvs[stack].rename(columns={
                id_col:db.KEY_PLANEID,
                stack_col:db.KEY_REFSTACKNAME,
                name_col: db.KEY_CHANNEL_NAME,
                type_col: db.KEY_CHANNEL_TYPE
            }, inplace = True)
            planes = planes.append(self.stack_csvs[stack])
        planes = planes.reset_index()
        del planes['index']
        # cast PlaneID to be identical to the one in Measurement:
        planes[db.KEY_PLANEID] = planes[db.KEY_PLANEID].apply(lambda x: 'c'+str(int(x)))

        planes = planes.append({db.KEY_PLANEID: OBJECTS_PLANEID,
                       db.KEY_REFSTACKNAME: OBJECTS_STACKNAME,
                       db.KEY_CHANNEL_NAME: OBJECTS_CHANNELNAME,
                       db.KEY_CHANNEL_TYPE: OBJECTS_CHANNELTYPE},
                               ignore_index=True)

        return planes

    def _generate_planemeta(self):
        refplanes = self._generate_refplanemeta()
        stack = self._generate_stack()
        refplanes = refplanes.set_index(db.KEY_REFSTACKNAME)
        planes = stack.join(refplanes, on=db.KEY_REFSTACKNAME)
        planes = planes.loc[:,[db.KEY_STACKNAME, db.KEY_REFSTACKNAME, db.KEY_PLANEID]]
        return planes

    def _write_image_table(self):
        """
        Generates the Image
        table and writes it to the database.
        """
        image = self._generate_image()
        query = self.main_session.query(db.Image).filter(
            db.Image.ImageNumber.in_(image[db.KEY_IMAGENUMBER].astype(str).unique())
        )
        self._add_generic_tuple(image, query, db.Image)

    def _generate_image(self):
        """
        Generates the Image
        table.
        """
        image = pd.DataFrame(self._images_csv[db.KEY_IMAGENUMBER])
        return image

    def _write_objects_table(self):
        """
        Generates and save the cell table
        """
        objects = self._generate_objects()
        query = self.main_session.query(db.Objects).filter(
            db.Objects.ImageNumber.in_(objects[db.KEY_IMAGENUMBER].astype(str).unique()),
            db.Objects.ObjectNumber.in_(objects[db.KEY_OBJECTNUMBER].astype(str).unique()),
            db.Objects.ObjectID.in_(objects[db.KEY_OBJECTID].astype(str).unique())
        )
        self._add_generic_tuple(objects, query, db.Objects)


    def _generate_objects(self):
        """
        Genertes the cell table
        """
        objects = pd.DataFrame(self._measurement_csv[[db.KEY_OBJECTNUMBER,
                                                      db.KEY_OBJECTID,
                                                      db.KEY_IMAGENUMBER]])
        return objects

    def _write_measurement_table(self, chunksize=100000):
        """
        Generates the Measurement, MeasurementType and MeasurementName
        tables and writes them to the database.
        The Measurement Table can contain an extremely high ammount of rows
        and can therefore be quite slow

        Args:
            chunksize: the ammount of rows written concurrently to the DB
        """

        measurements, measurements_names, measurements_types = \
        self._generate_measurements()
        self.add_measurements(measurements)

        name_query = self.main_session.query(db.MeasurementName).filter(
            db.MeasurementName.MeasurementName.in_(measurements_names[db.KEY_MEASUREMENTNAME].astype(str).unique())
        )
        self._add_generic_tuple(measurements_names, name_query, db.MeasurementName)

        type_query = self.main_session.query(db.MeasurementType).filter(
            db.MeasurementType.MeasurementType.in_(measurements_types[db.KEY_MEASUREMENTTYPE].astype(str).unique())
        )
        self._add_generic_tuple(measurements_types, type_query, db.MeasurementType)

        del self._measurement_csv

    def _generate_measurements(self):
        measurements = self._measurement_csv
        meta = pd.Series(measurements.columns.unique()).apply(
            lambda x: lib.find_measurementmeta(self._stacks, x,
                                               no_stack_str=OBJECTS_STACKNAME,
                                              no_plane_string=OBJECTS_PLANEID))
        meta.columns = ['variable', db.KEY_MEASUREMENTTYPE, db.KEY_MEASUREMENTNAME,
                        db.KEY_STACKNAME, db.KEY_PLANEID]
        measurements = pd.melt(measurements,
                               id_vars=[db.KEY_IMAGENUMBER,
                                        db.KEY_OBJECTNUMBER,'Number_Object_Number',
                                       db.KEY_OBJECTID],
                               var_name='variable', value_name='value')
        measurements = measurements.merge(meta, how='inner', on='variable')
        del measurements['variable']
        del measurements['Number_Object_Number']
        measurements_names = pd.DataFrame(measurements[db.KEY_MEASUREMENTNAME].unique())
        measurements_names.columns = [db.KEY_MEASUREMENTNAME]
        measurements_names = measurements_names.rename_axis('id')
        measurements_types = pd.DataFrame(measurements[db.KEY_MEASUREMENTTYPE].unique())
        measurements_types.columns = [db.KEY_MEASUREMENTTYPE]
        measurements_types = measurements_types.rename_axis('id')
        measurements = measurements.sort_values([db.KEY_IMAGENUMBER,
                                                 db.KEY_OBJECTNUMBER,
                                                 db.KEY_STACKNAME,
                                                 db.KEY_MEASUREMENTTYPE,
                                                 db.KEY_MEASUREMENTNAME,
                                                 db.KEY_PLANEID])

        return measurements, measurements_names, measurements_types

    def _generate_masks(self):
        cpconf = self.conf[conf.CPOUTPUT]
        objects = cpconf[conf.MEASUREMENT_CSV][conf.OBJECTS]
        prefix = cpconf[conf.IMAGES_CSV][conf.MASKFILENAME_PEFIX]
        dat_mask = {obj:
                    self._images_csv[
                        [db.KEY_IMAGENUMBER, prefix+obj]
                    ].rename(columns={prefix+obj: db.KEY_FILENAME})
         for obj in objects}
        dat_mask = pd.concat(dat_mask, names=[db.KEY_OBJECTID, 'idx'])
        dat_mask = dat_mask.reset_index(level=db.KEY_OBJECTID, drop=False)
        dat_mask = dat_mask.reset_index(drop=True)
        return dat_mask

    def _write_masks_table(self):
        masks = self._generate_masks()
        query = self.main_session.query(db.Masks).filter(
            db.Masks.ObjectID.in_(masks[db.KEY_OBJECTID].astype(str).unique()),
            db.Masks.ImageNumber.in_(masks[db.KEY_IMAGENUMBER].astype(str).unique())
        )
        self._add_generic_tuple(masks, query, db.Masks)

    def _generate_object_relations(self):
        conf_rel = self.conf[conf.CPOUTPUT][conf.RELATION_CSV]
        col_map = {conf_rel[c]: target for c, target in [
            (conf.OBJECTID_FROM, db.KEY_OBJECTID_FROM),
            (conf.OBJECTID_TO, db.KEY_OBJECTID_TO),
            (conf.OBJECTNUMBER_FROM, db.KEY_OBJECTNUMBER_FROM),
            (conf.OBJECTNUMBER_TO, db.KEY_OBJECTNUMBER_TO),
            (conf.IMAGENUMBER_FROM, db.KEY_IMAGENUMBER_FROM),
            (conf.IMAGENUMBER_TO, db.KEY_IMAGENUMBER_TO),
            (conf.RELATIONSHIP, db.KEY_RELATIONSHIP)]}
        dat_relations = self._relation_csv[list(col_map.keys())]
        dat_relations = (dat_relations.rename(columns=col_map)
                         )
        return dat_relations

    def _write_object_relations_table(self):
        relations = self._generate_object_relations()
        query = test.main_session.query(db.ObjectRelations).filter(
            db.ObjectRelations.ImageNumberFrom.in_(relations[db.KEY_IMAGENUMBER_FROM].astype(str).unique()),
            db.ObjectRelations.ObjectNumberFrom.in_(relations[db.KEY_OBJECTNUMBER_FROM].astype(str).unique()),
            db.ObjectRelations.ObjectIDFrom.in_(relations[db.KEY_OBJECTID_FROM].astype(str).unique()),
            db.ObjectRelations.ImageNumberTo.in_(relations[db.KEY_IMAGENUMBER_TO].astype(str).unique()),
            db.ObjectRelations.ObjectNumberTo.in_(relations[db.KEY_OBJECTNUMBER_TO].astype(str).unique()),
            db.ObjectRelations.ObjectIDTo.in_(relations[db.KEY_OBJECTID_TO].astype(str).unique()),
            db.ObjectRelations.Relationship.in_(relations[db.KEY_RELATIONSHIP].astype(str).unique())
        )
        self._add_generic_tuple(relations, query, db.ObjectRelations)

    def _write_pannel_table(self):
        pannel = self._generate_pannel_table()
        query = self.main_session.query(db.Pannel).filter(
            db.Pannel.Metal.in_(pannel[db.PANNEL_KEY_METAL].astype(str).unique()),
            db.Pannel.Target.in_(pannel[db.PANNEL_KEY_TARGET].astype(str).unique())
        )
        self._add_generic_tuple(pannel, query, db.Pannel)
    def _generate_pannel_table(self):

        csv_pannel = self.pannel
        conf_pannel = self.conf[conf.PANNEL_CSV]
        col_map = {conf_pannel[c]: target for c, target in [
            (conf.PANEL_CSV_CHANNEL_NAME, db.PANNEL_KEY_METAL),
            (conf.PANEL_CSV_DISPLAY_NAME, db.PANNEL_KEY_TARGET),
            (conf.PANEL_CSV_ILASTIK_NAME, db.PANNEL_COL_ILASTIK),
            (conf.PANEL_CSV_BARCODE_NAME, db.PANNEL_COL_BARCODE),
            (conf.PANEL_CSV_CLONE_NAME, db.PANNEL_COL_ABCLONE),
            (conf.PANEL_CSV_CONCENTRATION_NAME, db.PANNEL_COL_CONCENTRATION),
            (conf.PANEL_CSV_TARGET_NAME, db.PANNEL_KEY_TARGET),
            (conf.PANEL_CSV_TUBE_NAME, db.PANNEL_COL_TUBENUMBER)]}
        cols = [c for c in col_map]
        csv_pannel.drop(list(set(csv_pannel.columns) - set(cols)), axis=1, inplace=True)
        csv_pannel = csv_pannel.rename(columns=col_map)
        #correct conc to Float
        csv_pannel[db.PANNEL_COL_CONCENTRATION] = csv_pannel[db.PANNEL_COL_CONCENTRATION].apply(
            lambda x: float(re.findall(r"[-+]?\d*\.\d+|\d+", x)[0])
        )
        return csv_pannel

    #########################################################################
    #########################################################################
    #                       filter and dist functions:                      #
    #########################################################################
    #########################################################################



    #########################################################################
    #########################################################################
    #                           setter functions:                           #
    #########################################################################
    #########################################################################
    def add_measurements(self, measurements, replace=False,
        col_image = db.KEY_IMAGENUMBER,
        col_object_no = db.KEY_OBJECTNUMBER,
        col_object_id = db.KEY_OBJECTID,
        col_type = db.KEY_MEASUREMENTTYPE,
        col_name = db.KEY_MEASUREMENTNAME,
        col_plane = db.KEY_PLANEID,
        col_stackname = db.KEY_STACKNAME,
        col_value = db.KEY_VALUE,
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
            (col_image, db.KEY_IMAGENUMBER),
            (col_object_no, db.KEY_OBJECTNUMBER),
            (col_object_id, db.KEY_OBJECTID),
            (col_type, db.KEY_MEASUREMENTTYPE),
            (col_name, db.KEY_MEASUREMENTNAME),
            (col_plane, db.KEY_PLANEID),
            (col_stackname, db.KEY_STACKNAME),
            (col_value, db.KEY_VALUE)]}

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

            images = [int(c) for c in measurements[db.KEY_IMAGENUMBER].unique()]
            objects = [int(c) for c in measurements[db.KEY_OBJECTNUMBER].unique()]
            object_id = [str(c) for c in measurements[db.KEY_OBJECTID].unique()]
            measurement_type = [str(c) for c in measurements[db.KEY_MEASUREMENTTYPE].unique()]
            measurement_name = [str(c) for c in measurements[db.KEY_MEASUREMENTNAME].unique()]
            plane = [str(c) for c in measurements[db.KEY_PLANEID].unique()]
            stack = [str(c) for c in measurements[db.KEY_STACKNAME].unique()]

            query =  self.main_session.query(db.Measurement).filter(
                db.Measurement.ImageNumber.in_(images),
                db.Measurement.ObjectNumber.in_(objects),
                db.Measurement.ObjectID.in_(object_id),
                db.Measurement.MeasurementType.in_(measurement_type),
                db.Measurement.MeasurementName.in_(measurement_name),
                db.Measurement.PlaneID.in_(plane),
                db.Measurement.StackName.in_(stack)
            )

            (bak, un) =  self._add_generic_tuple(measurements, query, db.Measurement, replace=replace)
            bak_t.append(bak)
            un_t.append(un)
        return (bak_t, un_t)


    def _add_generic_tuple(self, data, query, table, replace=False):
        """add_generic_tuple
        adds tuples from date to the database and returns non stored or
        deleted values.

        Args:
            Pandas DataFrame data: dataframe containing the data. It is
                required to name the columns according to the db schema
            sqlalchemy query query: query object to retrieve existing tuples.
                best option: query for all keys!
            Sqlalchemy Table table: the table object to be added to
            bool replace: if existing tuples should be replaced

        Returns:
            Pandas.DataFrame containing the deleted tuples. These can be used
                to restore the old ones.
            Pandas.DataFrame containing the unstored rows

        """
        data = data.reset_index(drop=True)
        backup =  pd.read_sql(query.statement, self.db_conn)
        key_cols = [key.name for key in inspect(table).primary_key]
        if replace:


            query.delete(synchronize_session='fetch')
            self.main_session.commit()
            data.to_sql(con=self.db_conn, if_exists='append',
                             name=table.__tablename__, index=False)

            return backup, None
        else:
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

            storable.to_sql(con=self.db_conn, if_exists='append',
                             name=table.__tablename__, index=False)

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
        result = pd.read_sql(session.query(db.Pannel).statement,self.db_conn)
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
        result = pd.read_sql(session.query(db.Pannel).filter(db.Pannel.Target==name).statement,self.db_conn)
        if len(result) > 0:
            return (result[db.PANNEL_KEY_METAL], result)
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
        result = pd.read_sql(session.query(db.Pannel).filter(db.Pannel.Metal==metal).statement,self.db_conn)
        if len(result) > 0:
            return (result[db.PANNEL_KEY_TARGET], result)
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
        return self.get_table_data(db.TABLE_OBJECT,  **args)

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
            clause_dict = {db.KEY_STACKNAME: stack_name}

        #stack_name
        self._sqlgenerate_simple_query('Stack', columns=['Stack.*',
                                                'DerivedStack.RefStackName'],
                                       clause_dict=clause_dict)
        query += ' LEFT JOIN DerivedStack ON Stack.StackName = DerivedStack.RefStackName'
        print(query)
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
        return self.get_table_data(db.TABLE_MEASUREMENT,  **args)

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

    def _get_table_object(self, name):
        return getattr(db, name)

    def _get_column_from_table(self, table_obj, col_name):
        return getattr(table_obj, col_name)

    def _get_table_column(self, table_name, col_name):
        tab = self._get_table_object(table_name)
        col = self._get_column_from_table(tab, col_name)
        return col

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
