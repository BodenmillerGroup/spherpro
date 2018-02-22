import spherpro.bromodules.processing_debarcoding as processing_debarcoding
import spherpro.bromodules.processing_dist_rim as processing_dist_rim
import spherpro.bromodules.processing_nb_agg as processing_nb_agg
class Processing(object):
    def __init__(self, bro):
        self.debarcode = processing_debarcoding.Debarcode(bro)
        self.calculate_dist_rim = processing_dist_rim.CalculateDistRim(bro)
        self.nb_aggregation = processing_nb_agg.AggregateNeightbours(bro)
