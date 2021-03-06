import spherpro.bromodules.filter_measurements as filter_measurements
import spherpro.bromodules.filter_membership as filter_membership
import spherpro.bromodules.filter_objectfilters as filter_objectfilters


class Filters(object):
    def __init__(self, bro):
        self.membership = filter_membership.FilterMembership(bro)
        self.measurements = filter_measurements.FilterMeasurements(bro)
        self.objectfilterlib = filter_objectfilters.ObjectFilterLib(bro)
