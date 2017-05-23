import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import re

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db


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
            'sqlite': db.connect_sqlite,
            'mysql': db.connect_mysql
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
        with open(configpath, 'r') as stream:
            try:
                self.conf = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

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
        self.db_conn = self.connectors[self.conf['backend']](self.conf)

    ##########################################
    #   Helper functions used by readData:   #
    ##########################################

    def _read_experiment_layout(self):
        """
        reads the experiment layout as stated in the config
        and saves it in the datastore
        """
        sep = self.conf['layout_csv'].get('sep', ',')
        self.experiment_layout = pd.read_csv(
            self.conf['layout_csv']['path'], sep=sep
        ).set_index(
            [self.conf['layout_csv']['plate_col'], self.conf['layout_csv']['condition_col']]
        )

    def _read_barcode_key(self):
        """
        reads the barcode key as stated in the config
        and saves it in the datastore
        """
        sep = self.conf['barcode_csv'].get('sep', ',')
        self.barcode_key = pd.read_csv(
            self.conf['barcode_csv']['path'], sep=sep
        ).set_index(
            self.conf['barcode_csv']['well_col']
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
        sep = self.conf['cpoutput']['measurement_csv'].get('sep', ',')
        self._measurement_csv = pd.read_csv(
            join(self.conf['cp_dir'],self.conf['cpoutput']['measurement_csv']['path']),
            sep=sep
        )
        sep = self.conf['cpoutput']['cells_csv'].get('sep', ',')
        self._cells_csv = pd.read_csv(
            join(self.conf['cp_dir'],self.conf['cpoutput']['cells_csv']['path']),
            sep=sep
        )
        sep = self.conf['cpoutput']['images_csv'].get('sep', ',')
        self._images_csv = pd.read_csv(
            join(self.conf['cp_dir'], self.conf['cpoutput']['images_csv']['path']),
            sep=sep
        )
        sep = self.conf['cpoutput']['relation_csv'].get('sep', ',')
        self._relation_csv = pd.read_csv(
            join(self.conf['cp_dir'], self.conf['cpoutput']['relation_csv']['path']),
            sep=sep
        )

    def _read_stack_meta(self):
        """
        reads the stack meta as stated in the config
        and saves it in the datastore
        """
        sep = self.conf['stack_dir'].get('sep', ',')
        dir = self.conf['stack_dir']['path']
        match = re.compile("(.*)\.csv")
        stack_files = [f for f in listdir(dir) if isfile(join(dir, f))]
        stack_data = [pd.read_csv(join(dir,n), sep) for n in stack_files]
        stack_files = [match.match(name).groups()[0] for name in stack_files]
        self.stacks = {stack: data for stack, data in zip(stack_files, stack_data)}
        sep = self.conf['stack_relations'].get('sep', ',')
        self._stack_relation_csv = pd.read_csv(
            self.conf['stack_relations']['path'],
            sep=sep
        )

    def _populate_db(self):
        """
        writes the tables to the database
        """
        self.db_conn = self.connectors[self.conf['backend']](self.conf)
        self._generate_Stack()
        self._generate_Modifications()
        self._generate_planes()
        self._generate_images()
        self._generate_cells()
        self._generate_measurement()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _generate_Stack(self):
        """
        Writes the Stack table to the databse
        """

        stack_col = self.conf['stack_relations'].get('stack_col', 'Stack')

        data = pd.DataFrame(self._stack_relation_csv[stack_col])
        data = data.append({stack_col:'NoStack'}, ignore_index=True)
        data.columns = ['StackName']
        data = data.set_index("StackName")

        data.reset_index().to_sql(con=self.db_conn, if_exists='append', name="Stack", index=False)

    def _generate_Modifications(self):
        """
        Creates the StackModifications, StackRelations, Modifications,
        RefStack and DerivedStack tables and writes them to the database
        """

        parent_col = self.conf['stack_relations'].get('parent_col', 'Parent')
        modname_col = self.conf['stack_relations'].get('modname_col', 'ModificationName')
        modpre_col = self.conf['stack_relations'].get('modpre_col', 'ModificationPrefix')
        stack_col = self.conf['stack_relations'].get('stack_col', 'Stack')
        ref_col = self.conf['stack_relations'].get('ref_col', 'RefStack')

        stackrel = self._stack_relation_csv.loc[self._stack_relation_csv[parent_col]!='0']
        Modifications = pd.DataFrame(stackrel[modname_col])
        Modifications['tmp'] = stackrel[modpre_col]
        Modifications.columns = ['ModificationName','ModificationPrefix']
        Modifications = Modifications.set_index('ModificationName')
        Modifications.reset_index().to_sql(con=self.db_conn, if_exists='append', name="Modification", index=False)

        StackModification = pd.DataFrame(stackrel[stack_col])
        StackModification['ModificationName'] = stackrel[modname_col]
        StackModification['ParentStackName'] = stackrel[parent_col]
        StackModification.columns = ['ChildName','ModificationName','ParentName']
        StackModification = StackModification.set_index(['ChildName','ModificationName','ParentName'])
        StackModification.reset_index().to_sql(con=self.db_conn, if_exists='append', name="StackModification", index=False)

        ref_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]=='0']
        RefStack = pd.DataFrame(ref_stack[stack_col])
        RefStack.columns = ['StackName']
        RefStack = RefStack.set_index('StackName')
        RefStack.reset_index().to_sql(con=self.db_conn, if_exists='append', name="RefStack", index=False)


        derived_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]!='0']
        DerivedStack = pd.DataFrame(derived_stack[stack_col])
        DerivedStack['RefStackName'] = derived_stack[ref_col]
        DerivedStack.columns = ['StackName', 'RefStackName']
        DerivedStack = DerivedStack.set_index('StackName')
        DerivedStack.reset_index().to_sql(con=self.db_conn, if_exists='append', name="DerivedStack", index=False)

    def _generate_planes(self):
        """
        generates the PlaneMeta Table and writes it to the database.
        """

        stack_col = self.conf['stack_dir'].get('stack_col', 'StackName')
        id_col = self.conf['stack_dir'].get('id_col', 'index')
        name_col = self.conf['stack_dir'].get('name_col', 'name')
        type_col = self.conf['stack_dir'].get('type_col', 'channel_type')
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
        planes = planes.reset_index().rename_axis('id')
        del planes['index']
        # cast PlaneID to be identical to the one in Measurement:
        planes['PlaneID'] = planes['PlaneID'].apply(lambda x: 'c'+str(int(x)))

        planes.to_sql(con=self.db_conn, if_exists='append', name="PlaneMeta", index=False)


    def _generate_images(self):
        """
        Generates the Image
        table and writes it to the database.
        """
        image = pd.DataFrame(self._images_csv['ImageNumber'])
        image.to_sql(con=self.db_conn, if_exists='append', name="Image", index=False)


    def _generate_cells(self):
        """
        Generates the Cell
        table and writes it to the database.
        """
        cells = pd.DataFrame(self._cells_csv['ImageNumber'])
        cells['CellNumber'] = self._cells_csv['ObjectNumber']
        cells.to_sql(con=self.db_conn, if_exists='append', name="Cell", index=False)


    def _generate_measurement(self, chunksize=1000000):
        """
        Generates the Measurement, MeasurementType and MeasurementName
        tables and writes them to the database.
        The Measurement Table can contain an extremely high ammount of rows
        and can therefore be quite slow

        Args:
            chunksize: the ammount of rows written concurrently to the DB
        """
        stackgroup = '('
        for stack in [i for i in self.stacks]:
            if stackgroup == '(':
                stackgroup = stackgroup + stack
            else:
                stackgroup = stackgroup + '|' + stack
        stackgroup = stackgroup + ')'
        measurements = self._measurement_csv
        meta = pd.Series(measurements.columns.unique()).apply(lambda x: lib.find_measurementmeta(stackgroup,x))
        meta.columns = ['variable', 'MeasurementType', 'MeasurementName', 'StackName', 'PlaneID']
        measurements = pd.melt(measurements, id_vars=['ImageNumber', 'ObjectNumber','Number_Object_Number'],var_name='variable', value_name='value')
        measurements = measurements.merge(meta, how='inner', on='variable')
        measurements['CellNumber'] = measurements['ObjectNumber']
        del measurements['variable']
        del measurements['ObjectNumber']
        del measurements['Number_Object_Number']
        measurements_names = pd.DataFrame(measurements['MeasurementName'].unique())
        measurements_names.columns = ['MeasurementName']
        measurements_names.rename_axis('id').to_sql(con=self.db_conn, if_exists='append', name="MeasurementName")
        measurements_types = pd.DataFrame(measurements['MeasurementType'].unique())
        measurements_types.columns = ['MeasurementType']
        measurements_types.rename_axis('id').to_sql(con=self.db_conn, if_exists='append', name="MeasurementType")
        m = measurements.sort_values(['ImageNumber', 'CellNumber', 'StackName', 'MeasurementType', 'MeasurementName', 'PlaneID'])
        m.to_sql(con=self.db_conn, if_exists='append', name="Measurement", chunksize=chunksize, index=False)
        # remove measurement csv to avoid using memory
        del self._measurement_csv

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
        image_number = False,
        cell_number = False,
        measurement_type = False,
        measurement_name = False,
        stack_name = False,
        plane_id = False
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
            int/array image_number: ImageNumber. If 'False', do not filter
            int/array cell_number: CellNumber. If 'False', do not filter
            str/array measurement_type: MeasurementType. If 'False', do not filter
            str/array measurement_name: MeasurementName. If 'False', do not filter
            str/array stack_name: StackName. If 'False', do not filter
            str/array plane_id: PlaneID. If 'False', do not filter

        Returns:
            DataFrame containing:
            MeasurementName | MeasurementType | StackName
        """

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
        #measurement_type
        if type(measurement_type) is list:
            clause_tmp = 'MeasurementType IN ("'
            clause_tmp = clause_tmp+'","'.join(map(str, measurement_type))
            clause_tmp = clause_tmp+'")'
            clauses.append(clause_tmp)
        elif type(measurement_type) is str:
            clause_tmp = 'MeasurementType = "'
            clause_tmp = clause_tmp+str(measurement_type)+'"'
            clauses.append(clause_tmp)
        #measurement_name
        if type(measurement_name) is list:
            clause_tmp = 'MeasurementName IN ("'
            clause_tmp = clause_tmp+'","'.join(map(str, measurement_name))
            clause_tmp = clause_tmp+'")'
            clauses.append(clause_tmp)
        elif type(measurement_name) is str:
            clause_tmp = 'MeasurementName = "'
            clause_tmp = clause_tmp+str(measurement_name)+'"'
            clauses.append(clause_tmp)
        #stack_name
        if type(stack_name) is list:
            clause_tmp = 'StackName IN ("'
            clause_tmp = clause_tmp+'","'.join(map(str, stack_name))
            clause_tmp = clause_tmp+'")'
            clauses.append(clause_tmp)
        elif type(stack_name) is str:
            clause_tmp = 'StackName = "'
            clause_tmp = clause_tmp+str(stack_name)+'"'
            clauses.append(clause_tmp)
        #plane_id
        if type(plane_id) is list:
            clause_tmp = 'PlaneID IN ("'
            clause_tmp = clause_tmp+'","'.join(map(str, plane_id))
            clause_tmp = clause_tmp+'")'
            clauses.append(clause_tmp)
        elif type(plane_id) is str:
            clause_tmp = 'PlaneID = "'
            clause_tmp = clause_tmp+str(plane_id)+'"'
            clauses.append(clause_tmp)

        query = 'SELECT * FROM Measurement'
        for part in clauses:
            if query.split(' ')[-1] != 'Measurement':
                query = query + ' AND'
            else:
                query = query + ' WHERE'
            query = query + ' ' + part
        query = query+';'

        return pd.read_sql_query(query, con=self.db_conn)

    def get_(self, arg):
        pass
