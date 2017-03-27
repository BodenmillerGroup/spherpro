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
