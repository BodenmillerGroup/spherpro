import pandas as pd

import spherpro.bromodules.plot_base as base
import spherpro.configuration as conf
import spherpro.db as db

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

    def register_objects(self, object_meta, assume_new=False):
        """
        Registers an object metadata table
        Needs to have the columns:
            object_type
            image_id
            object_number
        Registers them and returns a table
            object_type
            image_id
            object_number
            object_id
            image_id

        If assume_new is True, the objects are assumed to be guaranteed
        new - use with care!
        """
        COL_OBJ_ID = db.objects.object_id.key
        img_dict = {n: i for n, i in
                    self.session.query(db.images.image_number,
                                       db.images.image_id)}
        object_meta[db.images.image_id.key] = object_meta[db.images.image_number.key].replace(img_dict)
        if assume_new == False:
            object_meta[COL_OBJ_ID] = [
                l[0] if l is not None else None
                for l in [self.session.query(COL_OBJ_ID)
                              .filter(db.objects.object_number == row[db.objects.object_number.key],
                                      db.objects.image_id == row[db.objects.image_id.key],
                                      db.objects.object_type == row[db.objects.object_type.key]).one_or_none()
                          for idx, row in object_meta.iterrows()]]
        else:
            object_meta[COL_OBJ_ID] = None
        fil = object_meta[COL_OBJ_ID].isnull()
        if sum(fil) > 0:
            object_meta.loc[fil, COL_OBJ_ID] = self.bro.data._query_new_ids(
                db.objects.object_id, sum(fil))
            # object_meta[COL_OBJ_ID] = object_meta[COL_OBJ_ID].astype(np.int)
            self.bro.data._bulkinsert(object_meta.loc[fil, :],
                                      db.objects)
        return object_meta

    def register_single_measurement(self, measurement_name, measurement_type, plane_id):
        dat_measure_meta = pd.DataFrame(
            {db.measurements.measurement_name.key: [measurement_name],
             db.measurements.plane_id.key: [plane_id],
             db.measurements.measurement_type.key: [measurement_type]
             })
        dat_measure_meta = self.register_measurements(dat_measure_meta)
        return dat_measure_meta[db.measurements.measurement_id.key].values[0]

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
            # measure_meta[MEAS_ID] = measure_meta[MEAS_ID].astype(int)
            self.bro.data._bulkinsert(measure_meta.loc[fil, :],
                                      db.measurements)
        return measure_meta

    def add_object_measurements(self, dat_meas, replace=True, drop_all_old=False):
        self.bro.io.objmeasurements.add_anndata_datmeasurements(dat_meas, replace=replace,
                                                                drop_all_old=drop_all_old)

    def delete_measurements_by_ids(self, meas_ids):
        q = (self.bro.session.query(db.object_measurements)
             .filter(db.object_measurements.measurement_id.in_([int(i) for i in meas_ids])))
        q.delete(synchronize_session=False)
        self.bro.session.commit()

    def get_object_plane_id(self):
        """
        Gets the object plane id, based on defaults.

        The object plane is a plane to capture object level measurements,
        not related to any channel, e.g. size/shape/location...
        """
        def_obj = self.bro.data.conf[conf.QUERY_DEFAULTS][conf.OBJECT_DEFAULTS]
        OUT_STACK = def_obj[conf.DEFAULT_STACK_NAME]
        OUT_CHANNEL_TYPE = def_obj[conf.DEFAULT_CHANNEL_TYPE]
        OUT_CHANNEL_NAME = def_obj[conf.DEFAULT_CHANNEL_NAME]
        plane_id = (self.bro.session.query(db.planes.plane_id)
                    .join(db.stacks)
                    .join(db.ref_stacks)
                    .join(db.ref_planes)
                    .filter(db.stacks.stack_name == OUT_STACK,
                            db.ref_planes.channel_type == OUT_CHANNEL_TYPE,
                            db.ref_planes.channel_name == OUT_CHANNEL_NAME)).one()[0]
        return plane_id
