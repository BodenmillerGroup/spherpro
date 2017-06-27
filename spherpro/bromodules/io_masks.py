"""
A class to generate handle the loading of the mask specified in the database.
"""
import spherpro.bromodules.io_base as io_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

class IoMasks(io_base.BaseIo)
    def __init__(self, bro):
        super().__init__(bro)

    def get_mask(image_number):

