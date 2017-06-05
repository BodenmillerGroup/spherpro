import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import re

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.configuration as conf

KEY_IMAGENUMBER = 'ImageNumber'
KEY_MEASUREMENTTYPE = 'MeasurementType'
KEY_MEASUREMENTNAME = 'MeasurementName'
KEY_STACKNAME = 'StackName'
KEY_PLANEID = 'PlaneID'

TABLE_MEASUREMENT = 'Measurement'

DICT_DB_KEYS = {
    'image_number': KEY_IMAGENUMBER,
    'cell_number': KEY_IMAGENUMBER,
    'measurement_type': KEY_MEASUREMENTTYPE,
    'measurement_name': KEY_MEASUREMENTNAME,
    'stack_name': KEY_STACKNAME,
    'plane_id': KEY_PLANEID,
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
        sep = self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV][conf.SEP]
        self._measurement_csv = pd.read_csv(
            join(self.conf[conf.CP_DIR],
                 self.conf[conf.CPOUTPUT][conf.MEASUREMENT_CSV][conf.PATH]),
            sep=sep
        )
        sep = self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.SEP]
        self._images_csv = pd.read_csv(
            join(self.conf[conf.CP_DIR],
                 self.conf[conf.CPOUTPUT][conf.IMAGES_CSV][conf.PATH]),
            sep=sep
        )
        sep = self.conf[conf.CPOUTPUT][conf.RELATION_CSV][conf.SEP]
        self._relation_csv = pd.read_csv(
            join(self.conf[conf.CP_DIR],
                 self.conf[conf.CPOUTPUT][conf.RELATION_CSV][conf.PATH]),
            sep=sep
        )

    def _read_stack_meta(self):
        """
        reads the stack meta as stated in the config
        and saves it in the datastore
        """
        sep = self.conf[conf.STACK_DIR][conf.SEP]
        stack_dir = self.conf[conf.STACK_DIR][conf.PATH]
        match = re.compile("(.*)\.csv")
        stack_files = [f for f in listdir(stack_dir) if isfile(join(stack_dir, f))]
        stack_data = [pd.read_csv(join(stack_dir,n), sep) for n in stack_files]
        stack_files = [match.match(name).groups()[0] for name in stack_files]
        self.stacks = {stack: data for stack, data in zip(stack_files, stack_data)}
        sep = self.conf[conf.STACK_RELATIONS][conf.SEP]
        self._stack_relation_csv = pd.read_csv(
            self.conf[conf.STACK_RELATIONS][conf.PATH],
            sep=sep
        )

    def _populate_db(self):
        """
        writes the tables to the database
        """
        self.db_conn = self.connectors[self.conf[conf.BACKEND]](self.conf)
        self._write_stack_table()
        self._write_modification_tables()
        self._write_planes_table()
        self._write_image_table()
        self._write_cells()
        self._write_measurement_table()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _write_stack_table(self):
        """
        Writes the Stack table to the databse
        """
        data = self._generate_stack()
        data.to_sql(con=self.db_conn, if_exists='append',
                                  name="Stack", index=False)

    def _generate_stack(self):
        """
        Generates the data for the Stack table
        """
        stack_col = self.conf[conf.STACK_RELATIONS][conf.STACK]
        data = pd.DataFrame(self._stack_relation_csv[stack_col])
        data = data.append({stack_col:'NoStack'}, ignore_index=True)
        data.columns = ['StackName']

        return data

    def _write_modification_tables(self):
        """
        Creates the StackModifications, StackRelations, Modifications,
        RefStack and DerivedStack tables and writes them to the database
        """

        modifications = self._generate_modifications()
        modifications.to_sql(con=self.db_conn, if_exists='append',
                             name="Modification", index=False)

        stackmodification = self._generate_stackmodification()

        stackmodification.to_sql(con=self.db_conn, if_exists='append',
                                 name="StackModification", index=False)

        RefStack = self._generate_refstack()
        RefStack.to_sql(con=self.db_conn, if_exists='append',
                        name="RefStack", index=False)

        DerivedStack = self._generate_derivedstack()
        DerivedStack.to_sql(con=self.db_conn, if_exists='append',
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
        RefStack.columns = ['StackName']
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
        DerivedStack['RefStackName'] = derived_stack[ref_col]
        DerivedStack.columns = ['StackName', 'RefStackName']

        return DerivedStack

    def _write_planes_table(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """
        planes = self._generate_planes()

        planes.to_sql(con=self.db_conn, if_exists='append', name="PlaneMeta", index=False)

    def _generate_planes(self):

        stack_col = self.conf[conf.STACK_DIR][conf.STACK]
        id_col = self.conf[conf.STACK_DIR][conf.ID]
        name_col = self.conf[conf.STACK_DIR][conf.NAME]
        type_col = self.conf[conf.STACK_DIR][conf.TYPE]
        planes = pd.DataFrame(columns=[
            'PlaneID',
            'RefStackName',
            'Name',
            'Type'
        ])
        for stack in self.stacks:
            self.stacks[stack].rename(columns={
                id_col:'PlaneID',
                stack_col:'RefStackName',
                name_col:'Name',
                type_col:'Type'
            }, inplace = True)
            planes = planes.append(self.stacks[stack])
        planes = planes.reset_index()
        del planes['index']
        # cast PlaneID to be identical to the one in Measurement:
        planes['PlaneID'] = planes['PlaneID'].apply(lambda x: 'c'+str(int(x)))

        return planes


    def _write_image_table(self):
        """
        Generates the Image
        table and writes it to the database.
        """
        image = self._generate_image()
        image.to_sql(con=self.db_conn, if_exists='append', name="Image", index=False)

    def _generate_image(self):
        """
        Generates the Image
        table.
        """
        image = pd.DataFrame(self._images_csv['ImageNumber'])
        return image

    def _write_cells(self):
        """
        Generates and save the cell table
        """
        cells = self._generate_cells()
        cells.to_sql(con=self.db_conn, if_exists='append', name='Cell',
                     index=False)

    def _generate_cells(self):
        """
        Genertes the cell table
        """
        cells = pd.DataFrame(self._measurement_csv['ImageNumber'])
        cells['CellNumber'] = self._measurement_csv['ObjectNumber']
        return cells

    def _write_measurement_table(self, chunksize=1000000):
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
        measurements.to_sql(con=self.db_conn, if_exists='append',
                            name="Measurement", chunksize=chunksize, index=False)
        measurements_names.to_sql(con=self.db_conn, if_exists='append',
                                  name="MeasurementName")
        measurements_types.to_sql(con=self.db_conn, if_exists='append',
                                     name="MeasurementType")
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
        meta.columns = ['variable', 'MeasurementType', 'MeasurementName',
                        'StackName', 'PlaneID']
        measurements = pd.melt(measurements,
                               id_vars=['ImageNumber',
                                        'ObjectNumber','Number_Object_Number'],
                               var_name='variable', value_name='value')
        measurements = measurements.merge(meta, how='inner', on='variable')
        measurements['CellNumber'] = measurements['ObjectNumber']
        del measurements['variable']
        del measurements['ObjectNumber']
        del measurements['Number_Object_Number']
        measurements_names = pd.DataFrame(measurements['MeasurementName'].unique())
        measurements_names.columns = ['MeasurementName']
        measurements_names = measurements_names.rename_axis('id')
        measurements_types = pd.DataFrame(measurements['MeasurementType'].unique())
        measurements_types.columns = ['MeasurementType']
        measurements_types = measurements_types.rename_axis('id')
        measurements = measurements.sort_values(['ImageNumber', 'CellNumber', 'StackName', 'MeasurementType', 'MeasurementName', 'PlaneID'])
        
        return measurements, measurements_names, measurements_types
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
        image_number = False):
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

        Returns:
            DataFrame
        """
        query = 'SELECT * FROM Image'

        clauses = []
        #image_number
        if type(image_number) is list:
            clause_tmp = 'ImageNumber IN ('
            clause_tmp = clause_tmp+','.join(map(str, image_number))
            clause_tmp = clause_tmp+')'
            clauses.append(clause_tmp)
        elif type(image_number) is int:
            clause_tmp = 'ImageNumber = '
            clause_tmp = clause_tmp+str(image_number)
            clauses.append(clause_tmp)

        for part in clauses:
            if query.split(' ')[-1] != 'Image':
                query = query + ' AND'
            else:
                query = query + ' WHERE'
            query = query + ' ' + part
        query = query+';'

        return pd.read_sql_query(query, con=self.db_conn)

    def get_cell_meta(self,
        image_number = False,
        cell_number = False):
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
            int/array cell_number: CellNumber. If 'False', do not filter

        Returns:
            DataFrame
        """
        query = 'SELECT * FROM Cell'

        clauses = []
        #image_number
        if type(image_number) is list:
            clause_tmp = 'ImageNumber IN ('
            clause_tmp = clause_tmp+','.join(map(str, image_number))
            clause_tmp = clause_tmp+')'
            clauses.append(clause_tmp)
        elif type(image_number) is int:
            clause_tmp = 'ImageNumber = '
            clause_tmp = clause_tmp+str(image_number)
            clauses.append(clause_tmp)

        #cell_number
        if type(cell_number) is list:
            clause_tmp = 'CellNumber IN ('
            clause_tmp = clause_tmp+','.join(map(str, cell_number))
            clause_tmp = clause_tmp+')'
            clauses.append(clause_tmp)
        elif type(cell_number) is int:
            clause_tmp = 'CellNumber = '
            clause_tmp = clause_tmp+str(cell_number)
            clauses.append(clause_tmp)

        for part in clauses:
            if query.split(' ')[-1] != 'Cell':
                query = query + ' AND'
            else:
                query = query + ' WHERE'
            query = query + ' ' + part
        query = query+';'

        return pd.read_sql_query(query, con=self.db_conn)

    def get_stack_meta(self,
        stack_name = False):
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
        # TODO: rewrite using the new helper functions
        query = 'SELECT Stack.*, DerivedStack.RefStackName FROM Stack'

        clauses = []
        #stack_name
        if type(stack_name) is list:
            clause_tmp = 'Stack.StackName IN ('
            clause_tmp = clause_tmp+','.join(map(str, stack_name))
            clause_tmp = clause_tmp+')'
            clauses.append(clause_tmp)
        elif type(stack_name) is int:
            clause_tmp = 'Stack.StackName = '
            clause_tmp = clause_tmp+str(stack_name)
            clauses.append(clause_tmp)


        for part in clauses:
            if query.split(' ')[-1] != 'Cell':
                query = query + ' AND'
            else:
                query = query + ' WHERE'
            query = query + ' ' + part

        query = query + ' LEFT JOIN DerivedStack ON Stack.StackName = DerivedStack.RefStackName'

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
        cell_number=None,
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
            int/array cell_number: CellNumber. If NONE, do not filter
            str/array measurement_type: MeasurementType. If NONE, do not filter
            str/array measurement_name: MeasurementName. If NONE, do not filter
            str/array stack_name: StackName. If NONE, do not filter
            str/array plane_id: PlaneID. If NONE, do not filter

        Returns:
            DataFrame containing:
            MeasurementName | MeasurementType | StackName
        """

        clauses = []
        
        args = locals()
        key_dict = lib.filter_and_rename_dict(args, DICT_DB_KEYS) 
        clauses = lib.construct_in_clause_list(key_dict)

        query = lib.construct_sql_query(TABLE_MEASUREMENT, columns=columns, clauses=clauses)
       
        return pd.read_sql_query(query, con=self.db_conn)

    def get_(self, arg):
        pass
print('test')
