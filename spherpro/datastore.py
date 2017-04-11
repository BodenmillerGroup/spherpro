import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import re
import sqlite3

import spherpro as spp
import spherpro.library as lib


class DataStore(object):
    """docstring for DataStore."""
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

        self.connectors = {'sqlite': self._connect_sqlite}

    def read_config(self, configpath):
        with open(configpath, 'r') as stream:
            try:
                self.conf = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def read_data(self):
        """read_data
        Reads the Data using the file locations given in the configfile.
        """
        # Read the data based on the config
        self._read_experiment_layout(self.conf['layout_csv'])
        self._read_barcode_key(self.conf['barcode_csv'])
        # self._readWellMeasurements(self.conf['wells_csv'])
        # self._read_cut_meta(self.conf['cut_csv'])
        # self._read_roi_meta(self.conf['roi_csv'])
        self._read_measurement_data()
        self._read_stack_meta()
        self._populate_db()

    ##########################################
    #   Helper functions used by readData:   #
    ##########################################

    def _read_experiment_layout(self, layoutfile):
        sep = layoutfile.get('sep', ',')
        self.experiment_layout = pd.read_csv(
            layoutfile['path'], sep=sep
        ).set_index(
            [layoutfile['plate_col'], layoutfile['condition_col']]
        )

    def _read_barcode_key(self, barcodefile):
        # Read and validate the barcode key
        sep = barcodefile.get('sep', ',')
        self.barcode_key = pd.read_csv(
            barcodefile['path'], sep=sep
        ).set_index(
            barcodefile['well_col']
        )

    def _read_well_measurements(self, wellmesfile):
        # Read and validate the well measurements
        raise NotImplementedError


    def _read_cut_meta(self, cutfile):
        # Read and validate the channel metadata
        raise NotImplementedError

    def _read_roi_meta(self, roifile):
        # Read and validate the ROI metadata
        raise NotImplementedError


    def _read_measurement_data(self):
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
        sep = self.conf['stack_dir'].get('sep', ',')
        dir = self.conf['stack_dir']['path']
        match = re.compile("(.*)\.csv")
        stack_files = [f for f in listdir(dir) if isfile(join(dir, f))]
        stack_data = [pd.read_csv(join(dir,n), sep) for n in stack_files]
        stack_files = [match.match(name).groups()[0] for name in stack_files]
        self.stacks = {stack: data for stack, data in zip(stack_files, stack_data)}
        sep = self.conf['stack_relations'].get('sep', ',')
        self.__stack_relation_csv = pd.read_csv(
            self.conf['stack_relations']['path'],
            sep=sep
        )

    def _populate_db(self):
        self.connectors[self.conf['backend']]()
        self._generate_Stack()
        self._generate_measurement()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _generate_Stack(self):
        data = pd.DataFrame(
            [{key: val for (key, val) in zip(["StackName"],[name])} for name in self.stacks]
        )
        data = data.set_index("StackName")

        data.to_sql(con=self.db_conn, name="Stack")

    def _generate_Modifications(self):
        raise NotImplementedError

    def _generate_RefStack(self):
        pass

    def _generate_DerivedStack(self):
        pass

    def _generate_measurement(self):
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
        measurements.to_sql(con=self.db_conn, name="Measurement")
    ##########################################
    #             Database access:           #
    ##########################################

    def _connect_sqlite(self):
        self.db_conn = sqlite3.connect(self.conf['sqlite']['db'])
