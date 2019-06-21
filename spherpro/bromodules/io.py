import spherpro.bromodules.io_masks as io_masks
import spherpro.bromodules.io_imcfolder as io_imc
class Io(object):
    def __init__(self, bro):
        self.masks = io_masks.IoMasks(bro)
        self.imcimg = io_imc.IoImc(bro)
