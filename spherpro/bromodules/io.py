import spherpro.bromodules.io_anndata as io_ann
import spherpro.bromodules.io_imcfolder as io_imc
import spherpro.bromodules.io_masks as io_masks
import spherpro.bromodules.io_stackimage as io_stackimage


class Io(object):
    def __init__(self, bro):
        self.masks = io_masks.IoMasks(bro)
        self.imcimg = io_imc.IoImc(bro)
        self.stackimg = io_stackimage.IoStackImage(bro)
        self.objmeasurements = io_ann.IoObjMeasurements(bro)
