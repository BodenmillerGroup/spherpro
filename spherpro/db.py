# Diffrent database collectors used by the datastore
# import pymysql
from sqlalchemy import Column, Integer, String, Float, Boolean, \
    ForeignKeyConstraint, UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Define the table and column names to be used
# These need to match the definitions bellow

def connect_sqlite(conf):
    """
    creates a sqlite connector to be used with the Datastore.

    Args:

    Returns:
        SQLite3 conne:ctor
    """
    db = conf['sqlite']['db']
    conn = 'sqlite:///%s' % (db)
    engine = create_engine(conn)
    Base.metadata.create_all(engine)
    return engine


def connect_mysql(conf):
    """
    creates a MySQL connector to be used with the Datastore and creates the
    Database tables.

    Args:
        conf: the config dictionnary from a Datastore object.

    Returns:
        MySQL connector
    """
    host = conf['mysql']['host']
    port = conf['mysql'].get('port', '3306')
    user = conf['mysql']['user']
    password = conf['mysql']['pass']
    database = conf['mysql']['db']
    conn = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, password, host, port, database)
    engine = create_engine(conn)
    return engine


def connect_postgresql(conf):
    """
    creates a MySQL connector to be used with the Datastore and creates the
    Database tables.

    Args:
        conf: the config dictionnary from a Datastore object.

    Returns:
        MySQL connector
    """
    conf_postgresql = 'postgresql'
    host = conf[conf_postgresql]['host']
    port = conf[conf_postgresql].get('port', '5432')
    user = conf[conf_postgresql]['user']
    password = conf[conf_postgresql]['pass']
    database = conf[conf_postgresql]['db']
    conn = 'postgresql+psycopg2://%s:%s@%s:%s/%s' % (user, password, host, port, database)
    engine = create_engine(conn)
    return engine


def initialize_database(engine):
    Base.metadata.create_all(engine)
    return engine


def drop_all(conn):
    """
    drops all tables

    Args:
        conn: the connector.
    """
    Base.metadata.drop_all(conn)


################################################################################
#                           Model Definitions                                  #
################################################################################

class sampleblocks(Base):
    """
    Represents a physical sample block which is cut and placed onto
    slides.
    """
    __tablename__ = 'sampleblocks'
    sampleblock_id = Column(Integer(), primary_key=True, autoincrement=True)
    sampleblock_name = Column(String(200))


class slides(Base):
    __tablename__ = 'slides'
    slide_id = Column(Integer(), primary_key=True, autoincrement=True)
    slide_number = Column(String(200))
    sampleblock_id = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
            [sampleblock_id],
            [sampleblocks.sampleblock_id]),
        {})


class conditions(Base):
    """docstring for images."""
    __tablename__ = 'conditions'
    condition_id = Column(Integer(), primary_key=True, autoincrement=True)
    condition_name = Column(String(200), server_default='default')
    time_point = Column(Float(), server_default='0.')
    barcode = Column(String(200))
    concentration = Column(Float(), server_default='0.')
    plate_id = Column(Integer(), server_default='1')
    bc_plate = Column(Integer(), server_default='0')
    bc_x = Column(Integer(), server_default='1')
    bc_y = Column(String(200), server_default='A')
    well_name = Column(String(200), server_default='A01')
    sampleblock_id = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
            [sampleblock_id],
            [sampleblocks.sampleblock_id]),
        {})


class slideacs(Base):
    __tablename__ = 'slideacs'
    slideac_id = Column(Integer(), primary_key=True, autoincrement=True)
    slide_id = Column(Integer())
    slideac_name = Column(String(200))
    slideac_folder = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
            [slide_id],
            [slides.slide_id]),
        {})


class sites(Base):
    """docstring for images."""
    __tablename__ = 'sites'
    site_id = Column(Integer(), primary_key=True, autoincrement=True)
    slideac_id = Column(Integer())
    site_mcd_panoramaid = Column(Integer())
    site_name = Column(String(200))
    site_pos_x = Column(Integer())
    site_pos_y = Column(Integer())
    site_shape_h = Column(Integer())
    site_shape_w = Column(Integer())
    site_panorama = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
            [slideac_id],
            [slideacs.slideac_id]),
        {})


class acquisitions(Base):
    __tablename__ = 'acquisitions'
    acquisition_id = Column(Integer(), primary_key=True, autoincrement=True)
    site_id = Column(Integer())
    acquisition_mcd_acid = Column(Integer())
    acquisition_mcd_roiid = Column(Integer())
    acquisition_pos_x = Column(Integer())
    acquisition_pos_y = Column(Integer())
    acquisition_shape_h = Column(Integer())
    acquisition_shape_w = Column(Integer())
    acquisition_image_before = Column(String(200))
    acquisition_image_after = Column(String(200))
    acquisition_image_file = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
            [site_id],
            [sites.site_id]),
        {})


class images(Base):
    """docstring for images."""
    __tablename__ = 'images'
    image_id = Column(Integer(), primary_key=True, autoincrement=True)
    image_number = Column(Integer())
    image_pos_x = Column(Integer())
    image_pos_y = Column(Integer())
    image_shape_h = Column(Integer())
    image_shape_w = Column(Integer())
    crop_number = Column(Integer())
    acquisition_id = Column(Integer())
    bc_depth = Column(Float())
    bc_invalid = Column(Integer())
    bc_valid = Column(Integer())
    bc_highest_count = Column(Integer())
    bc_second_count = Column(Integer())
    condition_id = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
            [condition_id],
            [conditions.condition_id]),
        ForeignKeyConstraint(
            [acquisition_id],
            [acquisitions.acquisition_id]),
        {})


TABLE_IMAGE = images.__tablename__


class masks(Base):
    """ a table describing the masks."""
    __tablename__ = 'masks'
    object_type = Column(String(200),
                         primary_key=True)
    image_id = Column(Integer(), primary_key=True)
    mask_filename = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [image_id],
        [images.image_id]),)


class objects(Base):
    """docstring for objects."""
    __tablename__ = 'objects'
    object_number = Column(Integer())
    object_id = Column(Integer(), primary_key=True, autoincrement=True)
    image_id = Column(Integer(), index=True)
    object_type = Column(String(200), index=True)
    __table_args__ = (ForeignKeyConstraint(
        [image_id],
        [images.image_id]),
                      ForeignKeyConstraint(
                          [object_type, image_id],
                          [masks.object_type, masks.image_id]), {})


class ref_stacks(Base):
    """docstring for ref_stacks."""
    __tablename__ = 'ref_stacks'
    ref_stack_id = Column(Integer(), primary_key=True, autoincrement=True)
    ref_stack_name = Column(String(200), unique=True)
    scale = Column(Float())


class ref_planes(Base):
    """docstring for planes."""
    __tablename__ = 'ref_planes'
    ref_stack_id = Column(Integer(), primary_key=True)
    ref_plane_number = Column(Integer(), primary_key=True)
    channel_type = Column(String(200))
    channel_name = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [ref_stack_id],
        [ref_stacks.ref_stack_id]), {})


class stacks(Base):
    """docstring for stacks."""
    __tablename__ = 'stacks'
    stack_id = Column(Integer(), primary_key=True, autoincrement=True)
    stack_name = Column(String(200), unique=True)
    ref_stack_id = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [ref_stack_id], [ref_stacks.ref_stack_id]), {})


class planes(Base):
    __tablename__ = 'planes'
    plane_id = Column(Integer(), primary_key=True, autoincrement=True)
    stack_id = Column(Integer())
    ref_plane_number = Column(Integer())
    ref_stack_id = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
            [ref_stack_id, ref_plane_number],
            [ref_planes.ref_stack_id, ref_planes.ref_plane_number]),
        ForeignKeyConstraint(
            [stack_id], [stacks.stack_id]),
        {})


class modifications(Base):
    """docstring for modifications."""
    __tablename__ = 'modifications'
    modification_id = Column(Integer(), primary_key=True, autoincrement=True)
    modification_name = Column(String(200), unique=True)
    modification_prefix = Column(String(200), unique=True)


class stack_modifications(Base):
    """docstring for stack_modifications."""
    __tablename__ = 'stack_modifications'
    modification_id = Column(Integer(),
                             primary_key=True)
    stack_id_parent = Column(Integer(), primary_key=True)
    stack_id_child = Column(Integer(), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            [stack_id_parent],
            [stacks.stack_id]),
        ForeignKeyConstraint(
            [stack_id_child],
            [stacks.stack_id]),
        ForeignKeyConstraint(
            [modification_id], [modifications.modification_id]),
        {})


class image_stacks(Base):
    """
    Represents an image stack linked to an image
    """
    __tablename__ = 'imagestacks'
    stack_id = Column(Integer(), primary_key=True)
    image_id = Column(Integer(), primary_key=True)
    image_stack_filename = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [stack_id], [stacks.stack_id]),
                      ForeignKeyConstraint([image_id], [images.image_id]), {})


class object_filter_names(Base):
    __tablename__ = 'object_filter_names'
    object_filter_id = Column(Integer(), primary_key=True, autoincrement=True)
    object_filter_name = Column(String(200), unique=True)


class object_filters(Base):
    __tablename__ = 'object_filters'
    object_filter_id = Column(Integer(), primary_key=True)
    object_id = Column(Integer(),
                       primary_key=True)
    filter_value = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [object_id],
        [objects.object_id]),
                      ForeignKeyConstraint([object_filter_id], [object_filter_names.object_filter_id]), {})


class object_relation_types(Base):
    __tablename__ = 'object_relation_types'
    object_relationtype_id = Column(Integer(), primary_key=True, autoincrement=True)
    object_relationtype_name = Column(String(200), index=True, unique=True)


class object_relations(Base):
    __tablename__ = 'object_relations'
    object_id_parent = Column(Integer(),
                              primary_key=True)
    object_id_child = Column(Integer(),
                             primary_key=True)
    object_relationtype_id = Column(Integer(), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [object_id_parent],
        [objects.object_id]),
                      ForeignKeyConstraint(
                          [object_id_child],
                          [objects.object_id]),
                      ForeignKeyConstraint(
                          [object_relationtype_id],
                          [object_relation_types.object_relationtype_id]),
                      {})


class measurement_names(Base):
    """
    Convenience table
    TODO: Consider removing
    """
    __tablename__ = 'measurement_names'
    measurement_name = Column(String(200), primary_key=True)


class measurement_types(Base):
    """
    Convenience table
    TODO: Consider removing
    """
    __tablename__ = 'measurement_types'
    measurement_type = Column(String(200), primary_key=True)


class measurements(Base):
    __tablename__ = 'measurements'
    measurement_id = Column(Integer(), primary_key=True, autoincrement=True)
    measurement_type = Column(String(200))
    measurement_name = Column(String(200))
    plane_id = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [measurement_name], [measurement_names.measurement_name]),
                      ForeignKeyConstraint([measurement_type], [measurement_types.measurement_type]),
                      ForeignKeyConstraint([plane_id], [planes.plane_id]),
                      UniqueConstraint(measurement_name, measurement_type, plane_id), {})


class object_measurements(Base):
    """docstring for object_measurements."""
    __tablename__ = 'object_measurements'
    measurement_id = Column(Integer(), primary_key=True)
    object_id = Column(Integer(),
                       primary_key=True)
    value = Column(Float(precision=32))
    __table_args__ = (ForeignKeyConstraint(
        [measurement_id], [measurements.measurement_id]),
                      ForeignKeyConstraint(
                          [object_id],
                          [objects.object_id]),
                      {})


class pannel(Base):
    """docstring for pannel."""
    __tablename__ = 'pannel'
    metal = Column(String(200), primary_key=True)
    target = Column(String(200), primary_key=True)
    antibody_clone = Column(String(200))
    concentration = Column(Float())
    is_ilastik = Column(Boolean())
    is_barcode = Column(Boolean())
    tube_number = Column(Integer())


class mask_measurements(Base):
    """docstring for image_measurements."""
    __tablename__ = 'mask_measurements'
    image_id = Column(Integer(), primary_key=True)
    object_type = Column(String(200),
                         primary_key=True)
    measurement_id = Column(Integer(), primary_key=True)
    value = Column(Float())
    __table_args__ = (
        ForeignKeyConstraint(
            [image_id, object_type], [masks.image_id, masks.object_type]),
        ForeignKeyConstraint(
            [measurement_id], [measurements.measurement_id]),
        {})


class image_measurements(Base):
    """docstring for image_measurements."""
    __tablename__ = 'image_measurements'
    image_id = Column(Integer(), primary_key=True)
    measurement_id = Column(Integer(), primary_key=True)
    value = Column(Float())
    __table_args__ = (
        ForeignKeyConstraint(
            [image_id], [images.image_id]),
        ForeignKeyConstraint(
            [measurement_id], [measurements.measurement_id]),
        {})


class valid_images(Base):
    __tablename__ = 'valid_images'
    image_id = Column(Integer(), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            [image_id], [images.image_id]), {})


class valid_objects(Base):
    __tablename__ = 'valid_objects'
    object_id = Column(Integer(), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
            [object_id], [objects.object_id]), {})
