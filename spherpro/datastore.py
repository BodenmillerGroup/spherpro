import pandas as pd
#import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import os
from odo import odo
import re
import io
import warnings
import tifffile as tif
# move to a  postgres specific location later!
from pgcopy import CopyManager

import numpy as np

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.bro as bro
import spherpro.configuration as conf
import spherpro.bromodules.processing
import spherpro.bromodules.io_anndata as io_anndata

from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
import sqlalchemy as sa
import logging

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
        #self._read_measurement_data()
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
        #self._read_experiment_layout()
        #self._read_barcode_key()
        # self._readWellMeasurements()
        # self._read_cut_meta()
        # self._read_roi_meta()
        #self._read_measurement_data()
        #self._read_stack_meta()
        self._read_pannel()
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            self._bulkinsert = self._bulk_pg_insert
        else:
            from odo import odo
        self.bro = bro.Bro(self)

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
            experiment_layout = pd.read_csv(
                self.conf[conf.LAYOUT_CSV][conf.PATH], sep=sep
            )
            # rename the columns
            rename_dict = {self.conf[conf.LAYOUT_CSV][c]: target for c, target in [
                    (conf.LAYOUT_CSV_COND_ID, db.conditions.condition_id.key),
                    (conf.LAYOUT_CSV_COND_NAME, db.conditions.condition_name.key),
                    (conf.LAYOUT_CSV_TIMEPOINT_NAME, db.conditions.time_point.key),
                    (conf.LAYOUT_CSV_BARCODE, db.conditions.barcode.key),
                    (conf.LAYOUT_CSV_CONCENTRATION_NAME, db.conditions.concentration.key),
                    (conf.LAYOUT_CSV_BC_PLATE_NAME, db.conditions.bc_plate.key),
                    (conf.LAYOUT_CSV_PLATE_NAME, db.conditions.plate_id.key),
                    (conf.LAYOUT_CSV_WELL_NAME, db.conditions.well_name.key)
                ]}
            experiment_layout = experiment_layout.rename(columns=rename_dict)
            self.experiment_layout = experiment_layout.fillna(0)
        else:
            self.experiment_layout = None

    def _read_barcode_key(self):
        """
        reads the barcode key as stated in the config
        """
        conf_bc = self.conf[conf.BARCODE_CSV]
        conf_layout = self.conf[conf.LAYOUT_CSV]
        path = conf_bc[conf.PATH]
        if path is not None:
            # Load the barcode key
            sep = conf_bc[conf.SEP]
            barcodes = pd.read_csv(
                path , sep=sep
            )
            # Adapt the names
            rename_dict = {
                    conf_bc[conf.BC_CSV_PLATE_NAME]:
                             conf_layout[conf.LAYOUT_CSV_BC_PLATE_NAME],
                    conf_bc[conf.BC_CSV_WELL_NAME]:
                             db.conditions.well_name.key
                             }
            barcodes = barcodes.rename(columns=rename_dict)
            # Convert the barcode key to a dictionary string
            barcodes = barcodes.set_index(
                list(rename_dict.values())
            )
            barcodes = (barcodes
                .transpose()
                # converts the barcodes to a string dictionary
                .apply(lambda x: str(x.to_dict())
                    )
                )
            barcodes = barcodes.rename(db.conditions.barcode.key)
            barcodes = barcodes.reset_index(drop=False)
            self.barcode_key = barcodes
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
        raise NotImplementedError

    def _read_objtype_measurements(self, object_type, chunksize):
        conf_meas = self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV]
        sep = conf_meas[conf.SEP]
        cpdir = self.conf[conf.CP_DIR]
        filetype = conf_meas[conf.FILETYPE]
        reader = pd.read_csv(os.path.join(cpdir, object_type+filetype),
                            sep=sep, chunksize=chunksize)

        if chunksize is None:
            reader = [reader]
        for dat_objmeas in reader:
            rename_dict = {
                self.conf[conf.OBJECTNUMBER]: db.objects.object_number.key,
                self.conf[conf.IMAGENUMBER]: db.images.image_number.key}
            dat_objmeas.rename(columns=rename_dict, inplace=True)
            dat_objmeas[db.objects.object_type.key] = object_type
            yield dat_objmeas

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

        self.bro = bro.Bro(self)
        self._write_imagemeta_tables()
        self._write_masks_table()
        self._write_stack_tables()
        self._write_refplanes_table()
        self._write_planes_table()
        self._write_pannel_table()
        self._write_condition_table()
        self._write_measurement_table(minimal)
        self.reset_valid_objects()
        self.reset_valid_images()
        #self._write_object_relations_table()
        # vacuum after population in postgres
        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            self._pg_vacuum()

    #### Helpers ####

    def replace_condition_table(self):
        """
        This is used in case an the experiment layout or
        barcoding is updated.

        Note that this will delete any debarcoding.
        """
        # read the tables
        self._read_experiment_layout()
        self._read_barcode_key()

        # delete the link between images and conditions
        session = self.main_session
        q = (session.query(db.images)
                .update({db.images.condition_id.key: None})
                )
        # delete the existing table
        (session.query(db.conditions)
                .delete()
                )
        session.commit()

        # write the table
        self._write_condition_table()
        session.commit()

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

    def _write_imagemeta_tables(self):
        """
        Write the tables containing the image metadata
        This contains:
            - the acquisition ROI 'roi': original acquisition where the image
                was cropped from
            - the site: the site on the slide where the acquisition ROI was made
                -> corresponds to a panroma in the MCD
            - the slide: the physical slide
        """
        dat_image = self._generate_image_table()
        dat_image, dat_roi = self._generate_roi_table(dat_image)
        dat_roi, dat_site = self._generate_site_table(dat_roi)
        dat_site, dat_slideac = self._generate_slideac_table(dat_site)
        dat_slideac, dat_slide = self._generate_slide_table(dat_slideac)
        dat_slide, dat_sampleblock = self._generate_sampleblock_table(dat_slide)

        self._bulkinsert(dat_sampleblock, db.sampleblocks)
        self._bulkinsert(dat_slide, db.slides)
        self._bulkinsert(dat_slideac, db.slideacs)
        self._bulkinsert(dat_site, db.sites)
        self._bulkinsert(dat_roi, db.acquisitions)
        self._bulkinsert(dat_image, db.images)
        self._bulkinsert(dat_image, db.valid_images)

    def _generate_image_table(self):
        """
        Generates the slide, site and roi metadata from the filenames or ome folders.
        """
        cpconf = self.conf[conf.CPOUTPUT]
        imgconf = cpconf[conf.IMAGES_CSV]
        objects = cpconf[conf.MEASUREMENT_CSV][conf.OBJECTS]
        obj = objects[0]
        prefix = cpconf[conf.IMAGES_CSV][conf.MASKFILENAME_PEFIX]
        #use any object to get a filename
        rename_dict = {
            prefix+obj: 'fn',
            imgconf[conf.IMAGE_HEIGHT_PREFIX]+obj: db.images.image_shape_h.key,
            imgconf[conf.IMAGE_WIDTH_PREFIX]+obj: db.images.image_shape_w.key,
        }
        dat_fn = self._images_csv.loc[:,
                                       [db.images.image_number.key] +
                                       list(rename_dict.keys())]
        dat_fn = dat_fn.rename(columns=rename_dict)


        re_meta = imgconf[conf.META_REGEXP]
        img_meta = lib.map_group_re(dat_fn['fn'], re_meta)
        img_meta.index = dat_fn.index
        img_meta = img_meta.join(dat_fn)

        colmap = {imgconf[g]: v for g, v in [
            (conf.GROUP_POSX, db.images.image_pos_x.key),
                  (conf.GROUP_POSY, db.images.image_pos_y.key),
                  (conf.GROUP_CROPID, db.images.crop_number.key),
                  (conf.GROUP_BASENAME, conf.GROUP_BASENAME)]
                  }
        img_meta = img_meta.rename(columns=colmap)
        img_meta[db.images.image_id.key] =\
            self._query_new_ids(db.images.image_id,
                                img_meta.shape[0])
        return img_meta

    def _generate_roi_table(self, img_meta):
        """
        Generates the ROi metadata table and updates the img_meta table to link
        to the roi_table
        """
        imgconf = self.conf[conf.CPOUTPUT][conf.IMAGES_CSV]
        roi_basenames = img_meta[conf.GROUP_BASENAME]
        roi_basenames = roi_basenames.drop_duplicates().values
        re_ome = imgconf[conf.IMAGE_OME_META_REGEXP]
        roi_meta = lib.map_group_re(roi_basenames, re_ome)
        roi_meta[conf.GROUP_BASENAME] = roi_basenames
        colmap = {imgconf[g]: v for g, v in [
            (conf.GROUP_SLIDEAC, db.slideacs.slideac_name.key),
                  (conf.GROUP_PANORMAID, db.sites.site_mcd_panoramaid.key),
                  (conf.GROUP_ACID, db.acquisitions.acquisition_mcd_acid.key),
                  (conf.GROUP_ROIID, db.acquisitions.acquisition_mcd_roiid.key)]
                  }
        roi_meta = roi_meta.rename(columns=colmap)
        # set default values
        roi_meta = roi_meta.reindex(columns=list(colmap.values())+[conf.GROUP_BASENAME],
                                    fill_value=np.NAN)
        roi_meta[db.acquisitions.acquisition_id.key] =\
            self._query_new_ids(db.acquisitions.acquisition_id,
                                roi_meta.shape[0])
        img_meta = pd.merge(img_meta,
                    roi_meta.loc[:, [db.acquisitions.acquisition_id.key,
                                     conf.GROUP_BASENAME]],
                            on=conf.GROUP_BASENAME)
        return img_meta, roi_meta

    def _generate_site_table(self, roi_meta):
        idvars = [db.slideacs.slideac_name.key,
                                     db.sites.site_mcd_panoramaid.key]
        site_meta = roi_meta.loc[:, idvars]
        site_meta = site_meta.drop_duplicates()

        site_meta[db.sites.site_id.key] =\
            self._query_new_ids(db.sites.site_id,
                                site_meta.shape[0])
        roi_meta = pd.merge(roi_meta,
                            site_meta.loc[:, idvars+[db.sites.site_id.key]],
                            on=idvars)
        return roi_meta, site_meta

    def _generate_slideac_table(self, site_meta):
        slideacs = site_meta[db.slideacs.slideac_name.key]
        slideacs = slideacs.drop_duplicates().values
        imgconf = self.conf[conf.CPOUTPUT][conf.IMAGES_CSV]
        re_slide = imgconf[conf.IMAGE_SLIDE_REGEXP]
        slideac_meta = lib.map_group_re(slideacs, re_slide)
        colmap = {imgconf[g]: v for g, v in [
            (conf.GROUP_SLIDENUMBER, db.slides.slide_number.key),
            (conf.GROUP_SAMPLEBLOCKNAME, db.sampleblocks.sampleblock_name.key)]
                  }
        slideac_meta = slideac_meta.rename(columns=colmap)
        # set default values
        slideac_meta = slideac_meta.reindex(columns=list(colmap.values()),
                                    fill_value=np.NAN)
        slideac_meta[db.slideacs.slideac_name.key] = slideacs

        slideac_meta[db.slideacs.slideac_id.key] =\
            self._query_new_ids(db.slideacs.slideac_id,
                                slideac_meta.shape[0])
        site_meta = pd.merge(site_meta,
                slideac_meta.loc[:, [db.slideacs.slideac_id.key,
                                     db.slideacs.slideac_name.key]],
                             on=db.slideacs.slideac_name.key)
        return site_meta, slideac_meta

    def _generate_slide_table(self, slideac_meta):
        slide_meta = slideac_meta.loc[:, [db.slides.slide_number.key,
                                          db.sampleblocks.sampleblock_name.key]]
        slide_meta = slide_meta.drop_duplicates()
        slide_meta[db.slides.slide_id.key] =\
            self._query_new_ids(db.slides.slide_id, slide_meta.shape[0])
        slideac_meta = pd.merge(slideac_meta,
                                slide_meta.loc[:, [db.slides.slide_number.key,
                                                   db.slides.slide_id.key,
                                                   db.sampleblocks.sampleblock_name.key]],
                                on=[db.slides.slide_number.key,
                                    db.sampleblocks.sampleblock_name.key])
        return slideac_meta, slide_meta

    def _generate_sampleblock_table(self, slide_meta):
        sampleblock_meta = slide_meta.loc[:, [db.sampleblocks.sampleblock_name.key]]
        sampleblock_meta = sampleblock_meta.drop_duplicates()
        sampleblock_meta[db.sampleblocks.sampleblock_id.key] =\
            self._query_new_ids(db.sampleblocks.sampleblock_id, sampleblock_meta.shape[0])
        slide_meta = pd.merge(slide_meta,
                                sampleblock_meta.loc[:,
                                    [db.sampleblocks.sampleblock_name.key,
                                     db.sampleblocks.sampleblock_id.key]],
                                on=db.sampleblocks.sampleblock_name.key)
        return slide_meta, sampleblock_meta

    def _generate_masks(self):
        cpconf = self.conf[conf.CPOUTPUT]
        objects = cpconf[conf.MEASUREMENT_CSV][conf.OBJECTS]
        prefix = cpconf[conf.IMAGES_CSV][conf.MASKFILENAME_PEFIX]
        dat_mask = {obj:
                    self._images_csv[
                        [db.images.image_number.key, prefix+obj]
                    ].rename(columns={prefix+obj: db.masks.mask_filename.key})
         for obj in objects}
        dat_mask = pd.concat(dat_mask, names=[db.objects.object_type.key, 'idx'])
        dat_mask = dat_mask.reset_index(level=db.objects.object_type.key, drop=False)
        dat_mask = dat_mask.reset_index(drop=True)
        #    if all(dat_mask[db.masks.shape_w.key].isnull()):
        #        """
        #        If the width and height are not in the regexp, load all the
        #        mask and check the width
        #        """
        #        cpconf = self.conf[conf.CPOUTPUT]
        #        basedir = cpconf[conf.IMAGES_CSV][conf.MASK_DIR]
        #        if basedir is None:
        #            basedir = self.conf[conf.CP_DIR]
        #        dat_mask[db.masks.shape_w.key], dat_mask[db.masks.shape_h.key] = \
        #                zip(*dat_mask[db.masks.file_name.key].map(lambda fn:
        #                        tif.imread(os.path.join(basedir, fn)).shape))
        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        dat_mask[db.masks.image_id.key] = dat_mask[db.images.image_number.key].replace(img_dict)
        return dat_mask

    def _write_masks_table(self):
        masks = self._generate_masks()
        self._bulkinsert(masks, db.masks)

    def _write_objects_table(self):
        """
        Generates and save the cell table
        """
        objects = self._generate_objects()
        self._bulkinsert(objects, db.objects)
        self._bulkinsert(objects, db.valid_objects)

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
        if self.conf[conf.BACKEND] == conf.CON_SQLITE:
            for obj_type, meas in self._generate_anndata_measurements():
                ioan = io_anndata.IoAnnData(self.bro, obj_type)
                ioan.initialize_anndata(meas)
        # increase performance
        if self.conf[conf.BACKEND] == conf.CON_MYSQL:
            measurements = self._generate_measurements(minimal, chuncksize=50000)
            self.db_conn.execute('SET FOREIGN_KEY_CHECKS = 0')
            self.db_conn.execute('SET UNIQUE_CHECKS = 0')
            for meas in measurements:
                self._bulkinsert(meas, db.object_measurements)
            self.db_conn.execute('SET FOREIGN_KEY_CHECKS = 1')
            self.db_conn.execute('SET UNIQUE_CHECKS = 1')


        if self.conf[conf.BACKEND] == conf.CON_POSTGRESQL:
            measurements = self._generate_measurements(minimal, chuncksize=50000)
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_pkey;')
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_measurement_id_fkey;')
            self.db_conn.execute('ALTER TABLE public.object_measurements DROP CONSTRAINT object_measurements_object_id_fkey;')
            for meas in measurements:
                logging.debug('Start inserting')
                self._bulk_pg_insert_numeric(meas, db.object_measurements)
                logging.debug('Start loading csv')
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


    def _register_measurement_meta(self, dat_meas):
        meas_cols = list(set(dat_meas.columns)-set([ db.objects.object_type.key,
              db.images.image_number.key,
              db.objects.object_number.key, db.objects.object_id.key]))
        meta = pd.Series(meas_cols).apply(
            lambda x: lib.find_measurementmeta(self._stacks, x,
                                               no_stack_str=OBJECTS_STACKNAME,
                                              no_plane_string=OBJECTS_PLANEID))
        meta.columns = ['variable',
                db.measurement_types.measurement_type.key,
                db.measurement_names.measurement_name.key,
                db.stacks.stack_name.key,
                db.ref_planes.ref_plane_id.key]
        meta = meta.loc[meta['variable'] != '', :]
        meta[db.ref_planes.ref_plane_id.key] = meta[db.ref_planes.ref_plane_id.key].map(lambda x: int(x.replace('c','')))

        dat_planeids = pd.read_sql(self.main_session.query(
                db.stacks.stack_name, db.planes.ref_plane_id, db.planes.plane_id)
            .join(db.planes).statement, self.db_conn)

        meta = meta.merge(dat_planeids)
        meta = self.bro.processing.measurement_maker.register_measurements(meta)
        return meta

    def _register_objects(self, dat_meas):
        """
        registers the objects
        """
        obj_metavars = [db.objects.object_number.key,
                db.objects.object_type.key,
                db.images.image_number.key]
        dat_objmeta = dat_meas.loc[:, obj_metavars]
        dat_objmeta = self.bro.processing.measurement_maker.register_objects(dat_objmeta, assume_new=True)
        return dat_objmeta

    def _generate_anndata_measurements(self):
        """
        Loads the measurements and converts them to the anndata format
        ->

        """
        conf_meas = self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV]
        for obj_type in conf_meas[conf.OBJECTS]:
            logging.debug(f'Read {obj_type}:')
            dat_meas = next(self._read_objtype_measurements(obj_type, chunksize=None))
            # register the measurements
            logging.debug('Register measurements:')
            dat_measmeta = self._register_measurement_meta(dat_meas)
            logging.debug('Register objects:')
            # register the objects
            # -> This adds objectid to the table
            dat_objmeta = self._register_objects(dat_meas)
            dat_meas = dat_meas.merge(dat_objmeta)
            # set the object_id as index
            dat_meas.set_index(db.objects.object_id.key, inplace=True)
            dat_meas.drop(columns=dat_objmeta.columns, errors='ignore', inplace=True)
            variables = dat_measmeta['variable']
            dat_meas = (dat_meas.loc[:, variables].rename(columns={v: int(i)
                            for v, i in zip(dat_measmeta['variable'],
                            dat_measmeta[db.measurements.measurement_id.key])}
                            ))
            yield (obj_type, dat_meas)


    def _generate_measurements(self, minimal,
                               chuncksize=3000,
                               longform=True):
        conf_meas = self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV]
        for obj_type in conf_meas[conf.OBJECTS]:
            logging.debug(f'Read {obj_type}:')
            for dat_meas in self._read_objtype_measurements(obj_type, chuncksize):
                # register the measurements
                logging.debug('Register measurements:')
                measurement_meta = self._register_measurement_meta(dat_meas)
                logging.debug('Register objects:')
                # register the objects
                object_meta = self._register_objects(dat_meas)
                logging.debug('Reshape values:')
                # get the measurement data
                variables = measurement_meta['variable']
                dat_measvals = dat_meas.loc[:, variables]
                value_col = dat_measvals.values.flatten(order='C')
                meas_id = measurement_meta[db.object_measurements.measurement_id.key].values
                meas_id = np.tile(meas_id, dat_meas.shape[0])
                obj_id = object_meta[db.object_measurements.object_id.key].values
                obj_id = np.repeat(obj_id, len(variables))
                logging.debug('Construct dataframe')
                measurements = pd.DataFrame({
                    db.object_measurements.object_id.key: obj_id,
                    db.object_measurements.measurement_id.key: meas_id,
                    db.object_measurements.value.key: value_col})
                logging.debug('Replace non finite')
                measurements[db.object_measurements.value.key].replace(np.inf, 2**16, inplace=True)
                measurements[db.object_measurements.value.key].replace(-np.inf, -(2**16), inplace=True)
                measurements.dropna(inplace=True)

                logging.debug('Start uploading')
                yield measurements

    def _generate_object_relation_types(self):
        dat_relations = (self._relation_csv)
        dat_types = pd.DataFrame(dat_relations.loc[:,
                db.object_relation_types.object_relationtype_name.key]).drop_duplicates()
        dat_types[db.object_relation_types.object_relationtype_id.key] = \
            self._query_new_ids(db.object_relation_types.object_relationtype_id, (dat_types.shape[0]))
        return dat_types

    def _generate_object_relations(self):
        logging.debug('Start img_dict')
        img_dict = {n: i for n, i in
                    self.main_session.query(db.images.image_number,
                                            db.images.image_id)}
        logging.debug('Start relation_dict')
        relation_dict = {n: i for n, i in
                         self.main_session.query(db.object_relation_types.object_relationtype_name,
                                                 db.object_relation_types.object_relationtype_id)}
        logging.debug('Start obj dict')
        obj_dict = {(imgid, objnr, objtype): objid
                    for imgid, objnr, objtype, objid in
                    self.main_session.query(db.objects.image_id,
                                            db.objects.object_number,
                                            db.objects.object_type,
                                            db.objects.object_id).all()}
        logging.debug('End obj dict')

        dat_relations = (self._relation_csv)
        logging.debug('Start replacing objfrom')
        dat_relations['timg'] = dat_relations[conf.IMAGENUMBER_FROM].replace(img_dict)
        dat_relations[db.object_relations.object_id_parent.key] =\
            dat_relations.loc[:,['timg',
                                 conf.OBJECTNUMBER_FROM,
                                 conf.OBJECTTYPE_FROM]].apply(
                lambda x: obj_dict.get((x[0], x[1], x[2])), axis=1)
        logging.debug('Start replacing ids objto')
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
        logging.debug('start generate object_relation_types')
        relation_types = self._generate_object_relation_types()
        self._bulkinsert(relation_types, db.object_relation_types)
        logging.debug('start generate object_relations')
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
        """
        Generates the condition metadata table based on a barcode and or a condition file
        """
        conf_layout = self.conf[conf.LAYOUT_CSV]
        exp_layout = self.experiment_layout
        barcode_key = self.barcode_key
        if (exp_layout is None) and (barcode_key is None):
            return None
        if exp_layout is None:
            data = barcode_key
        elif barcode_key is None:
            data = exp_layout
        else:
            data = exp_layout.merge(barcode_key)
        data = lib.fill_null(data, db.conditions)
        # Legacy: split barcode in x and y
        def get_y(x):
            return x[0]
        def get_x(x):
            return int(x[1:])
        data[db.conditions.bc_x.key] = data[db.conditions.well_name.key].map(get_x)
        data[db.conditions.bc_y.key] = data[db.conditions.well_name.key].map(get_y)

        # Assign the condition IDs:
        ncond = data.shape[0]
        data[db.conditions.condition_id.key] = self._query_new_ids(db.conditions.condition_id, ncond)

        # Add the sampleblock id:
        sample_dict = {n: i for n, i in
                    self.main_session.query(db.sampleblocks.sampleblock_name,
                                            db.sampleblocks.sampleblock_id)}
        if db.sampleblocks.sampleblock_name.key in data:
            # If sampleblocks are used, replace them by sample ids
            data[db.sampleblocks.sampleblock_id.key] = data[db.sampleblocks.sampleblock_name.key].replace(sample_dict)
        elif len(sample_dict) == 1:
            # If there is only one sample, assume this is the one that the conditions refer to
            data[db.sampleblocks.sampleblock_id.key] = list(sample_dict.values())[0]
        else:
            raise('''Sampleblocks are used in the `slide_regexp`, but not refered to in the experiment layout!\n
                    Either remove them in the regexp or add them to the layout!''')
        return data

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
            # this will instantiate the sequence if it is not
            # done so yet
            # otherwise it will lead to a gap of 1 between the ids
            #which should not matter
            _ = session.execute(sa.schema.Sequence(str_seq))

            session.execute('ALTER SEQUENCE ' + str_seq + ' INCREMENT ' + str(n))
            i = session.execute(sa.schema.Sequence(str_seq))
            session.execute('ALTER SEQUENCE ' + str_seq + ' INCREMENT ' + str(1))
            session.commit()
            return range(i-n+1, i+1)
        else:
            #self.main_session.query(func
            prev_max = self.main_session.query(sa.func.max(id_col)).scalar()
            if prev_max is None:
                prev_max = 0
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

        logging.debug('Insert table of dimension: '+ str(data.shape))
        data = self._clean_columns(data, table)
        odo(data, dbtable)
        self.main_session.commit()

    def _bulk_pg_insert(self, data, table, drop=False):
        if drop:
            session = self.main_session
            session.query(table).delete()
            session.commit()
        logging.debug('Insert table of dimension: '+str(data.shape))
        data = self._clean_columns(data, table)
        output = io.StringIO()
        # ignore the index
        data.to_csv(output, sep='\t', header=False, index=False)
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

    def _bulk_pg_insert_numeric(self, data, table, drop=False):
        if drop:
            session = self.main_session
            session.query(table).delete()
            session.commit()
        logging.debug('Insert table of dimension: ' + str(data.shape))
        data = self._clean_columns(data, table)
        conn = self.db_conn.raw_connection()
        table_name = table.__tablename__
        mgr = CopyManager(conn, table_name, list(data.columns))
        mgr.copy(data.to_records(index=False), io.BytesIO)
        conn.commit()

    def _clean_columns(self, data, table):
        data_cols = data.columns
        table_cols = table.__table__.columns.keys()
        uniq = list(set(table_cols)-set(data_cols))
        data = data.loc[:, table_cols]
        for un in uniq:
            data.loc[:, un] = None
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
        logging.debug("starting storing measurements...")
        while not finished:
            logging.debug("still need to store "+str(len(measurements_base))+" tuples!")
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


    def _add_generic_tuple(self, data, table, query=None, replace=False, backup=False, pg=False):
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
            if pg:
                self._bulk_pg_insert(data,table)
            else:
                self._bulkinsert(data, table)

            return backup, None
        else:
            backup =  pd.read_sql(query.statement, self.db_conn)
            current = backup.copy()
            if current.shape[0] == 0:
                # if nothing already in the database (=current empty), store everything
                storable = data
                # unstored will be an empty dataframe
                unstored = current
            else:
                zw = data[key_cols].append(current[key_cols]).drop_duplicates(keep=False)
                storable = data.merge(zw)
                unstored = data.merge(zw, how='outer')

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

            if pg:
                self._bulk_pg_insert(storable,table)
            else:
                self._bulkinsert(storable, table)


            return None, unstored

    def reset_valid_images(self):
        sel = sa.select([db.images.image_id]).where(~db.images.image_id.in_(self.main_session.query(db.valid_images.image_id)))
        ins = sa.insert(db.valid_images).from_select([db.valid_images.image_id.key], sel)
        self.main_session.execute(ins)
        self.main_session.commit()

    def reset_valid_objects(self):
        sel = sa.select([db.objects.object_id]).where(~db.objects.object_id.in_(self.main_session.query(db.valid_objects.object_id)))
        ins = sa.insert(db.valid_objects).from_select([db.valid_objects.object_id.key], sel)
        self.main_session.execute(ins)
        self.main_session.commit()

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

    def get_measurement_query(self, session=None, valid_objects=True, valid_images=True, scaled=True):
        """
        Returns a query object that queries table with the most important
        information do identify a measurement
        """
        if session is None:
            session = self.main_session

        if scaled:
            query = session.query(
                    db.object_measurements.measurement_id,
                    db.object_measurements.object_id,
                    (db.object_measurements.value *
                        db.ref_stacks.scale
                        ).label(db.object_measurements.value.key))
        else:
            query = (session.query(db.object_measurements))

        query = (query
                 .join(db.measurements)
                 .join(db.measurement_types)
                 .join(db.measurement_names)
                 .join(db.planes)
                 .join(db.stacks)
                 .join(db.ref_planes)
                 .join(db.ref_stacks)
                 .join(db.objects)
                 .join(db.images)
                 .join(db.acquisitions)
                 .join(db.sites)
                 .join(db.slideacs)
                 .join(db.slides)
                 .join(db.sampleblocks)
                )

        if valid_objects:
            query = query.join(db.valid_objects)
        if valid_images:
            query = query.join(db.valid_images)
        return query

    def get_measmeta_query(self, session=None):
        """
        Returns a measurement object that queries table with the most important
        information do identify a measurement.
        """
        if session is None:
            session = self.main_session

        query = (session.query(db.measurements)
                 .join(db.measurement_types)
                 .join(db.measurement_names)
                 .join(db.planes)
                 .join(db.stacks)
                 .join(db.ref_planes)
                 .join(db.ref_stacks)
                 )
        return query

    def get_objectmeta_query(self, session=None, valid_objects=True, valid_images=True):
        """
        Returns a query object that queries table with the most important
        information do identify an object.
        """
        if session is None:
            session = self.main_session

        query = (session.query(db.objects)
                 .join(db.images)
                 .join(db.acquisitions)
                 .join(db.sites)
                 .join(db.slideacs)
                 .join(db.slides)
                 .join(db.sampleblocks)
                )

        if valid_objects:
            query = query.join(db.valid_objects)
        if valid_images:
            query = query.join(db.valid_images)
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
                d = d.loc[:,~d.columns.duplicated()]
                return d
            return query_postgres
        else:
            def query_general(query):
                d = pd.read_sql(query.statement, self.db_conn)
                return d
            return query_general
