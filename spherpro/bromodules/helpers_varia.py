import spherpro.bromodules.plot_base as pltbase
import spherpro.db as db


class HelperDb(pltbase.BasePlot):
    def __init__(self, bro):
        super().__init__(bro)

    def get_target_by_channel(self, channel_name):
        target = (self.session.query(db.pannel.target)
            .filter(db.pannel.metal == channel_name).first())
        if target is None:
            target = [channel_name]
        return target[0]
