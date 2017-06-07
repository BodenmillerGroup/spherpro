import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import os
import re

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.configuration as conf

DICT_DB_KEYS = {
    'image_number': db.KEY_IMAGENUMBER,
    'object_number': db.KEY_OBJECTNUMBER,
    'measurement_type': db.KEY_MEASUREMENTTYPE,
    'measurement_name': db.KEY_MEASUREMENTNAME,
    'stack_name': db.KEY_STACKNAME,
    'plane_id': db.KEY_PLANEID,
    'object_id': db.KEY_OBJECTID
}


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
        self._read_stack_meta()
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)

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
        self.stacks = {stack: data for stack, data in zip(stack_files, stack_data)}
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
        self._write_stack_table()
        self._write_modification_tables()
        self._write_planes_table()
        self._write_image_table()
        self._write_objects_table()
        self._write_measurement_table()
        self._write_masks_table()
        self._write_object_relations_table()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _write_stack_table(self):
        """
        Writes the Stack table to the databse
        """
        data = self._generate_stack()
        data.to_sql(con=self.db_conn, if_exists='replace',
                                  name="Stack", index=False)

    def _generate_stack(self):
        """
        Generates the data for the Stack table
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        data = pd.DataFrame(self._stack_relation_csv[stack_col])
        data = data.append({stack_col:'NoStack'}, ignore_index=True)
        data.columns = [db.KEY_STACKNAME]

        return data

    def _write_modification_tables(self):
        """
        Creates the StackModifications, StackRelations, Modifications,
        RefStack and DerivedStack tables and writes them to the database
        """

        modifications = self._generate_modifications()
        modifications.to_sql(con=self.db_conn, if_exists='replace',
                             name="Modification", index=False)

        stackmodification = self._generate_stackmodification()

        stackmodification.to_sql(con=self.db_conn, if_exists='replace',
                                 name="StackModification", index=False)

        RefStack = self._generate_refstack()
        RefStack.to_sql(con=self.db_conn, if_exists='replace',
                        name="RefStack", index=False)

        DerivedStack = self._generate_derivedstack()
        DerivedStack.to_sql(con=self.db_conn, if_exists='replace',
                            name="DerivedStack", index=False)

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
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        
        stackrel = self._stack_relation_csv.loc[self._stack_relation_csv[parent_col]!='0']
        StackModification = pd.DataFrame(stackrel[stack_col])
        StackModification['ModificationName'] = stackrel[modname_col]
        StackModification['ParentStackName'] = stackrel[parent_col]
        StackModification.columns = ['ChildName','ModificationName','ParentName']
        return StackModification

    def _generate_refstack(self):
        """
        Generates the refstack table
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        modpre_col = self.conf[conf.STACK_RELATIONS][conf.MODPRE]
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]

        ref_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]=='0']
        RefStack = pd.DataFrame(ref_stack[stack_col])
        RefStack.columns = [db.KEY_STACKNAME]
        return RefStack

    def _generate_derivedstack(self):
        """
        Genes the DerivedStack 
        """
        parent_col = self.conf[conf.STACK_RELATIONS][conf.PARENT]
        modname_col = self.conf[conf.STACK_RELATIONS][conf.MODNAME]
        modpre_col = self.conf[conf.STACK_RELATIONS][conf.MODPRE]
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        ref_col = self.conf[conf.STACK_RELATIONS][conf.REF]
        
        derived_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]!='0']
        DerivedStack = pd.DataFrame(derived_stack[stack_col])
        DerivedStack[db.KEY_REFSTACKNAME] = derived_stack[ref_col]
        DerivedStack.columns = [db.KEY_STACKNAME, db.KEY_REFSTACKNAME]

        return DerivedStack


    def _write_planes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_planes()

        planes.to_sql(con=self.db_conn, if_exists='replace', name="PlaneMeta", index=False)

    def _generate_planes(self):

        stack_col = self.conf[conf.STACK_DIR][conf.STACK]
        id_col = self.conf[conf.STACK_DIR][conf.ID]
        name_col = self.conf[conf.STACK_DIR][conf.NAME]
        type_col = self.conf[conf.STACK_DIR][conf.TYPE]
        planes = pd.DataFrame(columns=[
            db.KEY_PLANEID,
            db.KEY_REFSTACKNAME,
            db.KEY_CHANNEL_NAME,
            db.KEY_CHANNEL_TYPE
        ])
        for stack in self.stacks:
            self.stacks[stack].rename(columns={
                id_col:db.KEY_PLANEID,
                stack_col:db.KEY_REFSTACKNAME,
                name_col: db.KEY_CHANNEL_NAME,
                type_col: db.KEY_CHANNEL_TYPE
            }, inplace = True)
            planes = planes.append(self.stacks[stack])
        planes = planes.reset_index()
        del planes['index']
        # cast PlaneID to be identical to the one in Measurement:
        planes[db.KEY_PLANEID] = planes[db.KEY_PLANEID].apply(lambda x: 'c'+str(int(x)))

        return planes


    def _write_image_table(self):
        """
        Generates the Image
        table and writes it to the database.
        """
        image = self._generate_image()
        image.to_sql(con=self.db_conn, if_exists='replace', name=db.TABLE_IMAGE, index=False)

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
        objects.to_sql(con=self.db_conn, if_exists='replace', name=db.TABLE_OBJECT,
                     index=False)

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
        measurements.to_sql(con=self.db_conn, if_exists='replace',
                            name=db.TABLE_MEASUREMENT, chunksize=chunksize, index=False)
        measurements_names.to_sql(con=self.db_conn, if_exists='replace',
                                  name=db.TABLE_MEASUREMENT_NAME,
                                 index=False)
        measurements_types.to_sql(con=self.db_conn, if_exists='replace',
                                     name=db.TABLE_MEASUREMENT_TYPE,
                                  index=False)
        del self._measurement_csv

    def _generate_measurements(self):
        stackgroup = '('
        for stack in [i for i in self.stacks]:
            if stackgroup == '(':
                stackgroup = stackgroup + stack
            else:
                stackgroup = stackgroup + '|' + stack
        stackgroup = stackgroup + ')'
        measurements = self._measurement_csv
        meta = pd.Series(measurements.columns.unique()).apply(
            lambda x: lib.find_measurementmeta(stackgroup,x))
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
        masks.to_sql(con=self.db_conn, if_exists='replace',
                                     name=db.TABLE_MASKS)

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
        relations.to_sql(con=self.db_conn, if_exists='replace',
                         name=db.TABLE_OBJECT_RELATIONS, index=False)
    #########################################################################
    #########################################################################
    #                       filter and dist functions:                      #
    #########################################################################
    #########################################################################






    #########################################################################
    #########################################################################
    #                           getter functions:                           #
    #########################################################################
    #########################################################################

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
    
    #Properties:
    @property
    def pannel(self):
        if self._pannel is None:
            self._read_pannel()
        return self._pannel
