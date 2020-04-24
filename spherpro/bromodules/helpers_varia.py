import sqlalchemy as sa

import spherpro.db as db


class HelperDb:
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

    def get_objnumbers(self, obj_ids):
        bro = self.bro
        id_dict = {i: n for i, n in
                   bro.session.query(
                       db.objects.object_id,
                       db.objects.object_number)
                       .filter(
                       db.objects.object_id
                           .in_(obj_ids)
                   ).all()
                   }
        return [id_dict[i] for i in obj_ids]

    def get_plane_id(self, stack_name: str, channel_name: str) -> int:
        """
        Get plane ID for given stack_name, channel_name
        Args:
            stack_name: A stack name
            channel_name: A channel name as given in the ref_planes table.

        Returns:
            The plane id

        """
        return (self.session.query(db.planes.plane_id)
                .join(db.ref_planes)
                .join(db.stacks)
                .filter(db.ref_planes.channel_name == channel_name)
                .filter(db.stacks.stack_name == stack_name)
                ).one()[0]

    def get_mask(self, img_id):
        bro = self.bro
        mask = bro.io.masks.get_mask(img_id)
        return mask

    def get_imc(self, img_id, channel):
        bro = self.bro
        return (bro.io.imcimg
                .get_imcimg(img_id)
                .get_img_by_metal(channel))

    def get_nb_dat(self, relationtype_name, obj_type=None, fil_query=None,
                   valid_obj_only=True):
        nbquery = (self.session.query(db.object_relations.object_id_parent,
                                      db.object_relations.object_id_child)
                   .join(db.object_relation_types)
                   .filter(db.object_relation_types.object_relationtype_name == relationtype_name)
                   )
        if valid_obj_only:
            c = sa.alias(db.valid_objects)
            p = sa.alias(db.valid_objects)
            nbquery = (nbquery
                       .filter(db.object_relations.object_id_child == c.c.object_id)
                       .filter(db.object_relations.object_id_parent == p.c.object_id)
                       )
        if obj_type is not None:
            nbquery = (nbquery
                       .join(db.objects,
                             db.objects.object_id == db.object_relations.object_id_parent)
                       .filter(db.objects.object_type == obj_type))

        if fil_query is not None:
            q_fil = fil_query.alias()
            nbquery = nbquery.filter(db.object_relations.object_id_child == fil_query.c.object_id,
                                     db.object_relations.object_id_parent == q_fil.c.object_id)
        return self.bro.doquery(nbquery)
