import pandas as pd
import numpy as np
import yaml

class DataStore(object):
    """docstring for DataStore."""
    def __init__(self):
        # init empty properties here
        self.ExperimentLayout = None
        self.BarcodeKey = None
        self.WellMeasurements = None
        self.CutMeta = None
        self.RoiMeta = None
        self.ChannelMeta = None
        self.SphereMeta = None
        self.MeasurementData = None

    def readConfig(self, configpath):
        with open(configpath, 'r') as stream:
            try:
                self.conf = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def readData(self):
        # Read the data based on the config
        self.readExperimentLayout(self.conf['layout_csv'])
        self.readBarcodeKey(self.conf['barcode_csv'])
        #self.readWellMeasurements(self.conf['wells_csv'])
        self.readChannelMeta(self.conf['channel_csv'])

    ##########################################
    #   Helper functions used by readData:   #
    ##########################################

    def readExperimentLayout(self, layoutfile):
        sep=','
        if 'sep' in layoutfile:
            sep=layoutfile['sep']
        self.ExperimentLayout = pd.read_csv(layoutfile['path'], sep=sep).set_index([layoutfile['plate_col'],layoutfile['condition_col']])

    def readBarcodeKey(self, barcodefile):
        # Read and validate the barcode key
        sep=','
        if 'sep' in barcodefile:
            sep=barcodefile['sep']
        self.BarcodeKey = pd.read_csv(barcodefile['path'], sep=sep).set_index(barcodefile['well_col'])

    def readWellMeasurements(self, wellmesfile):
        # Read and validate the well measurements
        raise NotImplementedError

    def readChannelMeta(self, channelfile):
        # Read and validate the channel metadata
        raise NotImplementedError

    def readCutlMeta(self, cutfile):
        # Read and validate the channel metadata
        raise NotImplementedError

    def readRoiMeta(self, roifile):
        # Read and validate the ROI metadata
        raise NotImplementedError

    def readChannelMeta(self, channelfile):
        # Read and validate the channel metadata
        raise NotImplementedError

    def readMeasurementData(self):
        # Read and validate the measurement data
        raise NotImplementedError

    def generateSphereMeta(self):
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
        # return a DF where Index is Spherenumber and rows are | Valid_BC_counts | highest_BC_counts | second_BC_count | well |
