"""
A class to generate and add filters to a filter table.
"""
import spherpro.bromodules.filter_base as filter_base
import pandas as pd
import numpy as np
import re

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

FILTERSTACKNAME = "FilterStack"
FILTERTYPENAME = "filter"


class CustomFilterStack(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)


    def create_filter_stack(self):
        """
        writes a new FilterStack to the database. added as a RefStack.
        """
        exists = self.data.main_session.query(db.RefStack).filter(db.RefStack.RefStackName == FILTERSTACKNAME).count()
        if exists == 0:
            # add to RefStack
            refstack = [{
                db.KEY_REFSTACKNAME: FILTERSTACKNAME,
                db.KEY_SCALE: 1
            }]
            refstack = pd.DataFrame(refstack)
            self.data._add_generic_tuple(refstack, db.RefStack)
            # add to Stack
            stack = [{
                db.KEY_REFSTACKNAME: FILTERSTACKNAME,
                db.KEY_STACKNAME: FILTERSTACKNAME
            }]
            stack = pd.DataFrame(stack)
            self.data._add_generic_tuple(stack, db.Stack)

        # Nothing is added to PlaneMeta or refplanemeta

    def _get_valueless_table(self):
        """
        Used to create a Valueless Table. The Table represents a db.Measurement
        table, missing the value, MEasurementType, MeasurementName and PlaneID-columns
        """
        q = self.data.main_session.query(db.Objects)
        objects = pd.read_sql_query(q.statement, self.data.db_conn)
        objects[db.KEY_STACKNAME] = FILTERSTACKNAME
        return objects


    def write_filter_to_db(self, filterdata, filtername, drop=None):
        """
        Writes a dataframe containing Filterdata to the DB.
        Args:
            filterdata: DataFrame containing the filterdata. needs to be in the
                        format returned by _get_valueless_table, just with an
                        added value table.
            filtername: String stating the Filtername
        """
        if drop is None:
            drop=False
        # self.create_filter_stack()
        # # get all filter channels and the number of the largest from RefPlaneMeta
        # q = self.data.main_session.query(db.RefPlaneMeta.PlaneID).filter(db.RefPlaneMeta.RefStackName == "FilterStack")
        # channels = pd.read_sql_query(q.statement,self.data.db_conn)
        # channels = list(channels[db.KEY_PLANEID])
        # nochannels = len(channels)
        # if nochannels == 0:
        #     channel = "c1"
        # else:
        #     curr = max([int(i[-1]) for i in channels])
        #     nex = curr + 1
        #     channel = "c"+str(nex)
        # # create RefPlaneMeta for next higher channel using filtername
        # refplanemeta = [{
        #     db.KEY_REFSTACKNAME: FILTERSTACKNAME,
        #     db.KEY_PLANEID: channel,
        #     db.KEY_CHANNEL_TYPE: FILTERTYPENAME,
        #     db.KEY_CHANNEL_NAME: filtername
        # }]
        # table = pd.DataFrame(refplanemeta)
        # self.data._bulkinsert(table, db.RefPlaneMeta)
        # # create PlaneMeta for next higher channel
        # planemeta = [{
        #     db.KEY_STACKNAME: FILTERSTACKNAME,
        #     db.KEY_PLANEID: channel,
        #     db.KEY_REFSTACKNAME: FILTERSTACKNAME
        # }]
        # table = pd.DataFrame(planemeta)
        # self.data._bulkinsert(table, db.PlaneMeta)

        #
        # select only database columns and write to the Database
        filterdata = filterdata[[db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER, db.KEY_OBJECTID, db.KEY_FILTERVALUE]]
        # filterdata[db.KEY_MEASUREMENTNAME] = FILTERTYPENAME
        # filterdata[db.KEY_MEASUREMENTTYPE] = FILTERTYPENAME
        filterdata[db.KEY_FILTERNAME] = filtername
        # filterdata[db.KEY_STACKNAME] = FILTERSTACKNAME
        filterdata = filterdata.dropna()
        self.data._add_generic_tuple(filterdata, db.Filters, replace=drop)
