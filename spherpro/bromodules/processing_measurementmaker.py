
import pandas as pd
import numpy as np
import re
import operator

import spherpro as sp
import spherpro.datastore as datastore
import spherpro.db as db
import sqlalchemy as sa

import spherpro.bromodules.plot_base as base

MEAS_ID = db.measurements.measurement_id.key
MEAS_NAME = db.measurements.measurement_name.key
MEAS_TYPE = db.measurements.measurement_type.key
MEAS_PLANE = db.measurements.plane_id.key

class MeasurementMaker(base.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)

    def register_measurement_name(self, new_measname):
        x = db.measurement_names()
        x.measurement_name = new_measname
        self.session.merge(x)
        self.session.commit()

    def register_measurement_type(self, new_meastype):
        x = db.measurement_types()
        x.measurement_type = new_meastype
        self.session.merge(x)
        self.session.commit()

    def register_measurements(self, measure_meta):
        """
        Retisters a table with measurments. The ID column
        will be added automatically.
        """
        measure_meta = measure_meta.copy()
        # make sure the names and types are present:
        for m in measure_meta[MEAS_NAME].unique():
            self.register_measurement_name(m)
        for m in measure_meta[MEAS_TYPE].unique():
            self.register_measurement_type(m)
        measure_meta[MEAS_ID] = [
            l[0] if l is not None else None
            for l in [self.session.query(db.measurements.measurement_id)
            .filter(db.measurements.measurement_name == row[MEAS_NAME],
                db.measurements.measurement_type == row[MEAS_TYPE],
                db.measurements.plane_id == row[MEAS_PLANE]).one_or_none()
                for idx, row in measure_meta.iterrows()]]

        fil = measure_meta[MEAS_ID].isnull()
        if sum(fil) > 0:
            measure_meta.loc[fil, MEAS_ID] = self.bro.data._query_new_ids(
                db.measurements.measurement_id, sum(fil))
            self.bro.data._bulk_pg_insert(measure_meta.loc[fil, :],
                                          db.measurements)
        return measure_meta

    def add_object_measurements(self, dat_meas, replace=True, drop_all_old=False ):
        if drop_all_old:
            self.delete_measurements_by_ids(dat_meas[db.measurements.measurement_id.key].unique())
        self.data._add_generic_tuple(dat_meas, db.object_measurements,
                                     replace=replace, pg=True)

    def delete_measurements_by_ids(self, meas_ids):
        q = (self.bro.session.query(db.object_measurements)
            .filter(db.object_measurements.measurement_id.in_([int(i) for i in meas_ids])))
        q.delete(synchronize_session=False)
        self.bro.session.commit()

