import pandas as pd
import numpy as np


class DataStore(object):
    """docstring for DataStore."""
    def __init__(self):
        super(DataStore, self).__init__()
        # init empty properties here
        ExperimentLayout = None
        BarcodeKey = None
        WellMeasurements = None
        CutMeta = None
        RoiMeta = None
        ChannelMeta = None
        SphereMeta = None
        MeasurementData = None

    def readConfig(self, configpath):
        # Read the config and save the properties
        raise NotImplementedError

    def readData(self):
        # Read the data based on the config
        raise NotImplementedError


    ##########################################
    #   Helper functions used by readData:   #
    ##########################################

    def readExperimentLayout(self, layoutfile):
        # Read and validate the experiment layout
        raise NotImplementedError

    def readBarcodeKey(self, barcodefile):
        # Read and validate the barcode key
        raise NotImplementedError

    def readWellMeasurements(self, wellmesfile):
        # Read and validate the well measurements
        raise NotImplementedError

    def readChannelMeta(self, channelfile):
        # Read and validate the channel metadata
        raise NotImplementedError

    def readChannelMeta(self, channelfile):
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
