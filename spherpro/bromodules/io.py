import spherpro.bromodules.io_anndata as io_ann
import spherpro.bromodules.io_imcfolder as io_imc
import spherpro.bromodules.io_masks as io_masks


class Io(object):
    def __init__(self, bro):
        self.masks = io_masks.IoMasks(bro)
        self.imcimg = io_imc.IoImc(bro)
        self.objmeasurements = io_ann.IoObjMeasurements(bro)
