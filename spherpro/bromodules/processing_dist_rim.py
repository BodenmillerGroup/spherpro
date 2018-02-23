import spherpro.bromodules.filter_base as filter_base
import spherpro.bromodules.filter_objectfilters as filter_objectfilters
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa


STACKNAME = "DistStack"
TYPENAME = "pixel_dist"
CHANNELNAME = "dist-rim"

class CalculateDistRim(object):
    """docstring for CalculateDistRim."""
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data
        self.custfilter = filter_objectfilters.ObjectFilterLib(bro)


