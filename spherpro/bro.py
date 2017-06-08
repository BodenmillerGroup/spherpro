import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore

def get_bro(fn_config):
    """
    Convenience function to get a bro with a datastore initialized
    with a config file
    """
    store = datastore.DataStore()
    store.read_config(fn_config)
    store.resume_data()
    bro = Bro(store)
    return bro


class Bro(object):
    """docstring for Bro."""

    def __init__(self, DataStore):
        self.DataStore = DataStore

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



    #########################################################################
    #########################################################################
    #                         Quick plot functions:                         #
    #########################################################################
    #########################################################################

    def draw_scatterplot(arg):
        raise NotImplementedError
