import pandas as pd
import numpy as np
import re

import spherpro as spp
import spherpro.library as lib
import spherpro.db as db
import spherpro.datastore.DataStore as datastore

class Bro(object):
    """docstring for Bro."""

    def __init__(self, DataStore):
        self.DataStore = DataStore

    #########################################################################
    #########################################################################
    #                         Quick plot functions:                         #
    #########################################################################
    #########################################################################
