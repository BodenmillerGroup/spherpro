import spherpro.bromodules.filter_membership as filter_membership
class Filters(object):
    def __init__(self, bro):
        self.issphere = filter_membership.FilterMembership(bro)
