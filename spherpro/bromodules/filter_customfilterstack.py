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
        exists = self.data.main_session.query(db.ref_stacks).filter(db.ref_stacks.ref_stack_name == FILTERSTACKNAME).count()
        if exists == 0:
            # add to RefStack
            refstack = [{
                db.ref_Stacks.ref_stack_name.key: FILTERSTACKNAME,
                db.ref_stacks.scale.key: 1
            }]
            refstack = pd.DataFrame(refstack)
            self.data._add_generic_tuple(refstack, db.ref_stacks)
            # add to Stack
            stack = [{
                db.ref_Stacks.ref_stack_name.key: FILTERSTACKNAME,
                db.stacks.stack_name.key: FILTERSTACKNAME
            }]
            stack = pd.DataFrame(stack)
            self.data._add_generic_tuple(stack, db.stacks)

        # Nothing is added to PlaneMeta or refplanemeta

    def _get_valueless_table(self):
        """
        Used to create a Valueless Table. The Table represents a db.object_measurements
        table, missing the value, MEasurementType, MeasurementName and PlaneID-columns
        """
        q = self.data.main_session.query(db.objects)
        objects = pd.read_sql_query(q.statement, self.data.db_conn)
        objects[db.stacks.stack_name.key] = FILTERSTACKNAME
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
        # q = self.data.main_session.query(db.ref_planes.ref_plane_id).filter(db.ref_planes.RefStackName == "FilterStack")
        # channels = pd.read_sql_query(q.statement,self.data.db_conn)
        # channels = list(channels[db.ref_planes.ref_plane_id.key])
        # nochannels = len(channels)
        # if nochannels == 0:
        #     channel = "c1"
        # else:
        #     curr = max([int(i[-1]) for i in channels])
        #     nex = curr + 1
        #     channel = "c"+str(nex)
        # # create RefPlaneMeta for next higher channel using filtername
        # refplanemeta = [{
        #     db.ref_Stacks.ref_stack_name.key: FILTERSTACKNAME,
        #     db.ref_planes.ref_plane_id.key: channel,
        #     db.ref_planes.channel_type.key: FILTERTYPENAME,
        #     db.ref_planes.channel_name.key: filtername
        # }]
        # table = pd.DataFrame(refplanemeta)
        # self.data._bulkinsert(table, db.ref_planes)
        # # create PlaneMeta for next higher channel
        # planemeta = [{
        #     db.stacks.stack_name.key: FILTERSTACKNAME,
        #     db.ref_planes.ref_plane_id.key: channel,
        #     db.ref_Stacks.ref_stack_name.key: FILTERSTACKNAME
        # }]
        # table = pd.DataFrame(planemeta)
        # self.data._bulkinsert(table, db.planes)

        #
        # select only database columns and write to the Database
        filterdata = filterdata[[db.images.image_id.key, db.objects.object_number.key, db.objects.object_id.key, db.object_filters.filter_value.key]]
        # filterdata[db.measurement_names.measurement_name.key] = FILTERTYPENAME
        # filterdata[db.measurement_types.measurement_type.key] = FILTERTYPENAME
        filterdata[db.object_filter_names.object_filter_name.key] = filtername
        # filterdata[db.stacks.stack_name.key] = FILTERSTACKNAME
        filterdata = filterdata.dropna()
        self.data._add_generic_tuple(filterdata, db.object_filters, replace=drop)
