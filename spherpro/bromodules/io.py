import spherpro.bromodules.io_masks as io_masks
class Io(object):
    def __init__(self, bro):
        self.masks = io_masks.IoMasks(bro)
