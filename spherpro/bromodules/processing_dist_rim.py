import spherpro.bromodules.filter_base as filter_base
import spherpro.bromodules.filter_customfilterstack as filter_customfilterstack
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
        self.custfilter = filter_customfilterstack.CustomFilterStack(bro)

    def include_dist_rim(self, ass_diam=None):
        """
        calculates the distance-to-rim and adds it to the datastore
        The Data will be added as a filter. This is because of time reasons
        and needs to be FIXED! to be added to the measurements!
        """
        dist_sphere = self._get_dists()
        dist_sphere[db.KEY_VALUE] = dist_sphere.groupby(db.KEY_IMAGENUMBER)[db.KEY_VALUE].apply(lambda x:self._calc_dist_rim(x, max(x), ass_diam))

        self._write_tables(dist_sphere)



    def _get_dists(self):
        dist_other_q  = (self.data.get_measurement_query()
                     .filter(
                         self.bro.filters.measurements.get_measurement_filter_statements(
                             channel_names=["dist-sphere"],
                             object_ids=['cell'],
                             stack_names=['DistStack'],
                             measurement_names=['MeanIntensity'],
                             measurement_types=['Intensity'],
                         ))
                    )
        dist_other = pd.read_sql_query(dist_other_q.statement, self.data.db_conn)

        return dist_other


    def _calc_dist_rim(self, dist, radius_cut, radius_sphere):
        if radius_cut > radius_sphere:
            radius_sphere = radius_cut
        real_dist = radius_sphere - np.sqrt(radius_sphere**2 - 2*radius_cut*dist + dist**2)
        return real_dist


    def _write_tables(self, filterdata):
        # get all filter channels and the number of the largest from RefPlaneMeta
        done = self.data.main_session.query(db.ref_planes.channel_name).filter(db.ref_planes.channel_name == CHANNELNAME).count()
        if done > 0:
            raise NameError('Please remove the current dist-rim first')
        q = self.data.main_session.query(db.ref_planes.ref_plane_id).filter(db.ref_planes.RefStackName == STACKNAME)
        channels = pd.read_sql_query(q.statement,self.data.db_conn)
        channels = list(channels[db.KEY_PLANEID])
        nochannels = len(channels)
        if nochannels == 0:
            channel = "c1"
        else:
            curr = max([int(i[-1]) for i in channels])
            nex = curr + 1
            channel = "c"+str(nex)
        # create RefPlaneMeta for next higher channel using CHANNELNAME
        refplanemeta = [{
            db.KEY_REFSTACKNAME: STACKNAME,
            db.KEY_PLANEID: channel,
            db.KEY_CHANNEL_TYPE: TYPENAME,
            db.KEY_CHANNEL_NAME: CHANNELNAME
        }]
        table = pd.DataFrame(refplanemeta)
        self.data._bulkinsert(table, db.ref_planes)
        # create PlaneMeta for next higher channel
        planemeta = [{
            db.KEY_STACKNAME: STACKNAME,
            db.KEY_PLANEID: channel,
            db.KEY_REFSTACKNAME: STACKNAME
        }]
        table = pd.DataFrame(planemeta)
        self.data._bulkinsert(table, db.planes)

        #
        # select only database columns and write to the Database
        filterdata = filterdata[[db.KEY_IMAGENUMBER, db.KEY_OBJECTNUMBER, db.KEY_OBJECTID, db.KEY_MEASUREMENTNAME, db.KEY_PLANEID,db.KEY_VALUE]]
        filterdata[db.KEY_MEASUREMENTNAME] = "MeanIntensity"
        filterdata[db.KEY_MEASUREMENTTYPE] = "intensity"
        filterdata[db.KEY_PLANEID] = channel
        filterdata[db.KEY_STACKNAME] = STACKNAME
        filterdata = filterdata.dropna()
        self.data._add_generic_tuple(filterdata, db.object_measurements)