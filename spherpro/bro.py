import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore


CHANNEL_DISTSPHERE = 'dist-sphere'

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
        self.filters = Filters(self)

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
    
"""
Define Filters
"""
class Filters(object):
    def __init__(self, bro):
        self.bro = bro

    def add_issphere(minfrac=0.01, min_vsother=0.5):
        col_issphere = 'is-sphere'
        col_isother = 'is-other'
        col_measure = 'MeanIntensity'
        col_stack = 'BinStack'
        outcol_issphere = 'is-sphere'
        outcol_isambiguous = 'is-ambiguous'
        non_zero_offset = 1/100000
        dat_filter = pd.read_sql(
                (session
                     .query(
                                 db.Measurement.ImageNumber,
                                 db.Measurement.ObjectID,
                                 db.Measurement.ObjectNumber,
                                 db.Measurement.Value,
                                 db.PlaneMeta.ChannelName,
                                 db.RefStack.Scale
                                   )
                     .filter(db.Measurement.MeasurementName==col_measure)
                     .filter(db.Measurement.StackName==col_stack)
                    .filter(db.PlaneMeta.ChannelName.in_([col_isother,col_issphere]))
                    .join(db.PlaneMeta)
                      .join(db.RefStack)
                     ).statement,
            store.db_conn)
        dat_filter[db.KEY_VALUE] = (dat_filter[db.KEY_VALUE] * dat_filter[db.KEY_SCALE])
        idx_cols = [c for c in dat_filter.columns
                    if c not in [db.KEY_VALUE, db.KEY_CHANNEL_NAME]]
        dat_filter = dat_filter.pivot_table(values=db.KEY_VALUE,
                               columns=[db.KEY_CHANNEL_NAME])
        pd.DataFrame({'is-sphere': (
            (dat_filter[col_issphere]+non_zero_offset)/(
                dat_filer[col_isother]+non_zero_offset) > minfrac)})
            

