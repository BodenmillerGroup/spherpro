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
        self.measurement_data = None

        self.connectors = {
            'sqlite': db.connect_sqlite,
            'mysql': db.connect_mysql
        }

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
        self._read_measurement_data()
        self._read_stack_meta()

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
        self._generate_measurement()
        self._generate_planes()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _generate_Stack(self):
        """
        Writes the Stack table to the databse
        """

        stack_col = self.conf['stack_relations'].get('stack_col', 'Stack')

        data = pd.DataFrame(self._stack_relation_csv[stack_col])
        data.columns = ['StackName']
        data = data.set_index("StackName")

        data.reset_index().to_sql(con=self.db_conn, name="Stack")

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
        Modifications.reset_index().to_sql(con=self.db_conn, name="Modification")

        StackModification = pd.DataFrame(stackrel[stack_col])
        StackModification['ModificationName'] = stackrel[modname_col]
        StackModification['ParentStackName'] = stackrel[parent_col]
        StackModification.columns = ['StackName','ModificationName','ParentStackName']
        StackModification = StackModification.set_index(['StackName','ModificationName','ParentStackName'])
        StackModification.reset_index().to_sql(con=self.db_conn, name="StackModification")

        ref_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]=='0']
        RefStack = pd.DataFrame(ref_stack[stack_col])
        RefStack.columns = ['RefStackName']
        RefStack = RefStack.set_index('RefStackName')
        RefStack.reset_index().to_sql(con=self.db_conn, name="RefStack")


        derived_stack = self._stack_relation_csv.loc[self._stack_relation_csv[ref_col]!='0']
        DerivedStack = pd.DataFrame(derived_stack[stack_col])
        DerivedStack['RefStackName'] = derived_stack[ref_col]
        DerivedStack.columns = ['DerivedStackName', 'RefStackName']
        DerivedStack = DerivedStack.set_index('DerivedStackName')
        DerivedStack.reset_index().to_sql(con=self.db_conn, name="DerivedStack")

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

        planes.to_sql(con=self.db_conn, name="PlaneMeta", )

    def _generate_measurement(self):
        """
        Generates the Measurement, MeasurementType and MeasurementName
        tables and writes them to the database.
        The Measurement Table can contain an extremely high ammount of rows
        and can therefore be quite slow
        """
        stackgroup = '('
        for stack in [i for i in self.stacks]:
            if stackgroup == '(':
                stackgroup = stackgroup + stack
            else:
                stackgroup = stackgroup + '|' + stack
        stackgroup = stackgroup + ')'
        measurements = self._cells_csv
        meta = pd.Series(measurements.columns.unique()).apply(lambda x: lib.find_measurementmeta(stackgroup,x))
        meta.columns = ['variable', 'MeasurementType', 'MeasurementName', 'StackName', 'PlaneID']
        measurements = pd.melt(measurements, id_vars=['ImageNumber', 'ObjectNumber','Number_Object_Number'],var_name='variable', value_name='value')
        measurements = measurements.merge(meta, how='inner', on='variable')
        del measurements['variable']
        measurements_names = pd.DataFrame(measurements['MeasurementName'].unique())
        measurements_names.columns = ['MeasurementName']
        measurements_names.rename_axis('id').to_sql(con=self.db_conn, name="MeasurementName")
        measurements_types = pd.DataFrame(measurements['MeasurementType'].unique())
        measurements_types.columns = ['MeasurementType']
        measurements_types.rename_axis('id').to_sql(con=self.db_conn, name="MeasurementType")
        measurements.reset_index().rename_axis('id')
        measurements.to_sql(con=self.db_conn, name="Measurement", chunksize=1000000)
