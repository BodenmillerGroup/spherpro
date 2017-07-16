import spherpro.bromodules.filter_membership as filter_membership
import spherpro.bromodules.filter_measurements as filter_measurements
import spherpro.bromodules.filter_customfilterstack as filter_customfilterstack
import spherpro.bromodules.filter_stack_hq as filter_stack_hq
class Filters(object):
    def __init__(self, bro):
        self.issphere = filter_membership.FilterMembership(bro)
        self.measurements = filter_measurements.FilterMeasurements(bro)
        self.customfilterstack = filter_customfilterstack.CustomFilterStack(bro)
        self.stack_hq = filter_stack_hq.StackHQ(bro)
