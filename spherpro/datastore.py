import pandas as pd
import numpy as np
import yaml


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
            self.conf['cp_dir']+self.conf['cpoutput']['cells_csv']['path'],
            sep=sep
        )
        sep = self.conf['cpoutput']['images_csv'].get('sep', ',')
        self._images_csv = pd.read_csv(
            self.conf['cp_dir']+self.conf['cpoutput']['images_csv']['path'],
            sep=sep
        )
        sep = self.conf['cpoutput']['relation_csv'].get('sep', ',')
        self._relation_csv = pd.read_csv(
            self.conf['cp_dir']+self.conf['cpoutput']['relation_csv']['path'],
            sep=sep
        )

    def generate_sphere_meta(self):
        # Generate the sphere metadata
        # 1. empty DF
        # 2. debarcode and merge info (WellID, PlateID)
        # 3. Merge meta info about spheres
        #
        raise NotImplementedError

    ##########################################
    #               Debarcoding:             #
    ##########################################

    def debarcode(self):
        # Use data in self for debarcoding
        raise NotImplementedError
        # return a DF where Index is Spherenumber and rows are |
        # Valid_BC_counts  |highest_BC_counts | second_BC_count | well |
