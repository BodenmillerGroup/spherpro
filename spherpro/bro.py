import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import spherpro.bromodules.filters as filters
import spherpro.bromodules.plots as plots
import spherpro.bromodules.io as io
import spherpro.bromodules.processing as processing

import sqlalchemy as sa

import plotnine as gg


def get_bro(fn_config):
    """
    Convenience function to get a bro with a datastore initialized
    with a config file
    Args:
        fn_confio: path to the config file
    Returns:
        A true bro
    """
    store = datastore.DataStore()
    store.read_config(fn_config)
    store.resume_data()
    bro = Bro(store)
    return bro


class Bro(object):
    """docstring for Bro."""

    def __init__(self, DataStore):
        self.data = DataStore
        self.filters = filters.Filters(self)
        self.io = io.Io(self)
        self.plots = plots.Plots(self)
        self.processing = processing.Processing(self)
        self.doquery = self.data.get_query_function()
        self.session = self.data.main_session
    #########################################################################
    #########################################################################
    #                         preparation functions:                        #
    #########################################################################
    #########################################################################

    def calculate_dist_rim(self, assumed_spheresize = None):
        """calculate_dist_rim
        calculates the distance to rim and saves it to the DB.
        By default, we calculate the distance using the real sphere radius
        saved in the Image meta.
        If you add the assumed_spheresize, then an equal real radius is assumed
        for all spheres.
        The distance_to_rim is calculated using the MeanIntensity of the DistStack.

        Args:
            assumed_spheresize:
                - If None, then take sphere radius from Well images
                - If Float, then assume this for all spheres
                to be the real radius
                - If 'estimate', the real radius is estimated by assuming
                the largest sphere has the correct radius
        """

        if assumed_spheresize is None:
            raise NotImplementedError('This feature is not available yet!')
        elif (type(assumed_spheresize) is float) | (type(assumed_spheresize) is int):
            print("assuming size you gave me!")
        elif (type(assumed_spheresize) is str) & (assumed_spheresize == 'estimate'):
            print("assuming size from all images!")
        else:
            raise NameError('Please specify a valid option!')



    @property
    def is_debarcoded(self):
        isdeb = False
        q =self.data.main_session.query(db.images.condition_id)
        q = q.filter(db.images.condition_id.isnot(None)).count()
        if q > 0:
            isdeb = True
        return isdeb
