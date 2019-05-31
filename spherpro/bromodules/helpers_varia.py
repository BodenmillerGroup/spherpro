import spherpro.db as db

class HelperDb(pltbase.BasePlot):
    def __init__(self, bro):
        self.bro = bro
        self.session = self.bro.data.main_session
        self.data = self.bro.data

    def get_target_by_channel(self, channel_name):
        target = (self.session.query(db.pannel.target)
            .filter(db.pannel.metal == channel_name).first())
        if target is None:
            target = [channel_name]
        return target[0]

    def get_objnumbers(self, obj_id):
        bro = self.bro
        id_dict = {i: n for i, n in
                        bro.session.query(
                            db.objects.object_id,
                            db.objects.object_number)
                            .filter(
                                db.objects.object_id
                                .in_(obj_id)
                                ).all()
                                        }
        return [id_dict[i] for i in obj_id]

    def get_mask(self, img_id):
        bro = self.bro
        mask = bro.io.masks.get_mask(img_id)
        return mask

    def get_imc(self, img_id, channel):
        bro = self.bro
        return (bro.io.imcimg
                .get_imcimg(img_id)
                .get_img_by_metal(channel))
