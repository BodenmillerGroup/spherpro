import pandas as pd
import numpy as np
import yaml
from os import listdir
from os.path import isfile, join
import re
import sqlite3


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

    def _populate_db(self):
        self.connectors[self.conf['backend']]()
        self._generate_Stack()

    ##########################################
    #        Database Table Generation:      #
    ##########################################

    def _generate_Stack(self):
        match_normal = re.compile('.*_([a-zA-Z0-9]+)_c[0-9]+')
        stacks_normal = [match_normal.search(string).groups()[0] for string in self._cells_csv.columns.unique() if match_normal.search(string) is not None]
        stacks_normal = list(set(stacks_normal))

        match_special = re.compile('.*_([a-zA-Z0-9]+)_[0-9]+')
        stacks_special = [match_special.search(string).groups()[0] for string in self._cells_csv.columns.unique() if match_special.search(string) is not None]
        stacks_special = list(set(stacks_special))

        data = pd.DataFrame(
            [{key: val for (key, val) in zip(["StackName","Special"],[name,0])} for name in stacks_normal]
        )
        data = data.append(
            pd.DataFrame([{key: val for (key, val) in zip(["StackName","Special"],[name,1])} for name in stacks_special])
        )
        data = data.set_index("StackName")

        data.to_sql(con=self.db_conn, name="Stack")


    def _generate_RefStack(self):
        pass

    def _generate_DerivedStack(self):
        pass


    ##########################################
    #             Database access:           #
    ##########################################

    def _connect_sqlite(self):
        self.db_conn = sqlite3.connect(self.conf['sqlite']['db'])
