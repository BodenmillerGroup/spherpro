import spherpro.bromodules.filter_membership as filter_membership
import spherpro.bromodules.filter_measurements as filter_measurements
import spherpro.bromodules.filter_customfilterstack as filter_customfilterstack
class Filters(object):
    def __init__(self, bro):
        self.issphere = filter_membership.FilterMembership(bro)
        self.measurements = filter_measurements.FilterMeasurements(bro)
        self.customfilterstack = filter_customfilterstack.CustomFilterStack(bro) 
