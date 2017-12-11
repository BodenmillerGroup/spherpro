# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Boolean,\
     ForeignKeyConstraint, UniqueConstraint
Base = declarative_base()

# Define the table and column names to be used
# These need to match the definitions bellow

KEY_CHANNEL_NAME = 'channel_name'
KEY_CHANNEL_TYPE = 'channel_type'
KEY_CHILDNAME = 'ChildName'
KEY_CHILDID = 'ChildID'
KEY_DISPLAY_NAME = 'DisplayName'
KEY_FILENAME = 'FileName'
KEY_IMAGENUMBER = 'ImageNumber'
KEY_IMAGENUMBER_FROM = 'ImageNumberFrom'
KEY_IMAGENUMBER_TO = 'ImageNumberTo'
KEY_MEASUREMENTNAME = 'MeasurementName'
KEY_MEASUREMENTTYPE = 'MeasurementType'
KEY_MEASURMENTID = 'MeasurementID'
KEY_MODIFICATIONNAME = 'ModificationName'
KEY_MODIFICATIONPREFIX = 'ModificationPrefix'
KEY_MODIFICATIONID = 'ModificationID'
KEY_OBJECTID = 'ObjectID'
KEY_OBJECTNUMBER = 'ObjectNumber'
KEY_OBJECTUNIID = 'ObjectUniID'
KEY_OBJECTUNIID_FROM = 'ObjectIDFrom'
KEY_OBJECTUNIID_TO = 'ObjectIDTo'
KEY_PARENTNAME = 'ParentName'
KEY_PARENTID = 'ParentID'
KEY_PLANEID = 'PlaneID'
KEY_PLANEUNIID = 'PlaneUniID'
KEY_REFSTACKNAME = 'RefStackName'
KEY_REFSTACKID = 'RefStackID'
KEY_RELATIONSHIP = 'Relationship'
KEY_RELATIONSIPID = 'RelationshipID'
KEY_SCALE = 'Scale'
KEY_STACKNAME = 'StackName'
KEY_STACKID = 'StackID'
KEY_VALUE = 'Value'
KEY_CROPID = 'CropID'
KEY_POSX = 'PosX'
KEY_POSY = 'PosY'
KEY_SHAPEW = 'ShapeW'
KEY_SHAPEH = 'ShapeH'

KEY_FILTERNAME = 'FilterName'
KEY_FILTERVALUE = 'FilterValue'
KEY_FILTERID = 'FitlerID'

KEY_CONDITIONID = 'ConditionID'
KEY_CONDITIONNAME = 'ConditionName'
KEY_CONCENTRATIONNAME = 'Concentration'
KEY_TIMEPOINT = 'TimePoint'
KEY_BCPLATENAME = 'BCPlate'
KEY_BC = 'BarCode'
KEY_BCX = 'BCX'
KEY_BCY = 'BCY'
KEY_PLATEID = 'PlateID'

KEY_SITENAME = 'SiteName'
KEY_BCDEPTH = 'BCDepth'
KEY_BCINVALID = 'BCInvalid'
KEY_BCVALID = 'BCValid'
KEY_BCHIGHESTCOUNT = 'BCHighestCount'
KEY_BCSECONDCOUNT = 'BCSecondCount'


TABLE_CONDITION = 'conditions'
TABLE_SITE = 'sites'

TABLE_DERIVEDSTACK = 'DerivedStack'
TABLE_FILTERS = 'object_filters'
TABLE_FILTER_NAMES = 'object_filter_names'
TABLE_IMAGE = 'images'
TABLE_MASKS = 'masks'
TABLE_MEASUREMENT = 'object_measurements'
TABLE_MEASUREMENT_META = 'measurements'
TABLE_MEASUREMENT_NAME = 'measurement_names'
TABLE_MEASUREMENT_TYPE = 'measurement_types'
TABLE_MODIFICATION = 'modifications'
TABLE_OBJECT = 'objects'
TABLE_OBJECT_RELATIONS = 'object_relations'
TABLE_OBJECT_RELATIONS_TYPES = 'ObjectRelationsTypes'
TABLE_PLANEMETA = 'planes'
TABLE_REFPLANEMETA = 'ref_planes'
TABLE_REFSTACK = 'ref_stacks'
TABLE_STACK = 'stacks'
TABLE_STACKMODIFICATION = 'stack_modifications'
TABLE_IMAGEMEASUREMENT = 'image_measurements'


def connect_sqlite(conf):
    """
    creates a sqlite connector to be used with the Datastore.

    Args:
        conf: the config dictionnary from a Datastore object.

    Returns:
        SQLite3 conne:ctor
    """
    db=conf['sqlite']['db']
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
    db = conf['mysql']['db']
    conn = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, password, host, port, db)
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
    CON_POSTGRESQL = 'postgresql'
    host = conf[CON_POSTGRESQL]['host']
    port = conf[CON_POSTGRESQL].get('port', '5432')
    user = conf[CON_POSTGRESQL]['user']
    password = conf[CON_POSTGRESQL]['pass']
    db = conf[CON_POSTGRESQL]['db']
    conn = 'postgresql+psycopg2://%s:%s@%s:%s/%s' % (user, password, host, port, db)
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
class conditions(Base):
    """docstring for images."""
    __tablename__ = TABLE_CONDITION
    condition_id = Column(String(200), primary_key=True)
    condition_name = Column(String(200), server_default='default')
    time_point = Column(Float(), server_default="0")
    barcode = Column(String(200))
    concentration = Column(Float())
    plate_id = Column(Integer())
    bc_plate = Column(Integer())
    bc_x = Column(Integer())
    bc_y = Column(String(200))

class sites(Base):
    """docstring for images."""
    __tablename__ = TABLE_SITE
    site_name = Column(String(200), primary_key=True)



class images(Base):
    """docstring for images."""
    __tablename__ = TABLE_IMAGE
    image_id = Column(Integer(), primary_key=True)
    image_number = Column(Integer())
    bc_depth = Column(Float())
    bc_invalid = Column(Integer())
    bc_valid = Column(Integer())
    bc_highest_count = Column(Integer())
    bc_second_count = Column(Integer())
    condition_id = Column(String(200))
    site_name = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
        [condition_id],
        [conditions.condition_id]),
        ForeignKeyConstraint(
        [site_name],
        [sites.site_name]),
            {})

class masks(Base):
    """ a table describing the masks."""
    __tablename__ = TABLE_MASKS
    object_type = Column(String(200),
                       primary_key=True)
    image_id = Column(Integer(),  primary_key=True)
    pos_x = Column(Integer())
    pos_y = Column(Integer())
    shape_h = Column(Integer())
    shape_w = Column(Integer())
    crop_number = Column(Integer())
    file_name = Column(String(200))

class objects(Base):
    """docstring for objects."""
    __tablename__ = TABLE_OBJECT
    object_number = Column(Integer())
    object_id = Column(Integer(), primary_key=True, autoincrement=True)
    image_id = Column(Integer(), index=True)
    object_type = Column(String(200), index=True)
    __table_args__ = (ForeignKeyConstraint(
        [image_id],
        [images.image_id]),
        ForeignKeyConstraint(
        [object_type, image_id],
        [masks.object_type, masks.image_id]),{})

class ref_stacks(Base):
    """docstring for ref_stacks."""
    __tablename__ = TABLE_REFSTACK
    ref_stack_id = Column(Integer(), primary_key=True, autoincrement=True)
    ref_stack_name = Column(String(200), unique=True)
    scale = Column(Float())

class ref_planes(Base):
    """docstring for planes."""
    __tablename__ = TABLE_REFPLANEMETA
    ref_stack_id = Column(Integer(), primary_key=True)
    ref_plane_id = Column(Integer(), primary_key=True, autoincrement=True)
    channel_type = Column(String(200))
    channel_name = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [ref_stack_id],
        [ref_stacks.ref_stack_id]),{})

class stacks(Base):
    """docstring for stacks."""
    __tablename__ = TABLE_STACK
    stack_id = Column(Integer(), primary_key=True, autoincrement=True)
    stack_name = Column(String(200), unique=True)
    ref_stack_id = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [ref_stack_id], [ref_stacks.ref_stack_id]), {})

class planes(Base):
    __tablename__ = TABLE_PLANEMETA
    plane_id = Column(Integer(), primary_key=True, autoincrement=True)
    stack_id = Column(Integer())
    ref_plane_id = Column(Integer())
    ref_stack_id = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
        [ref_stack_id, ref_plane_id],
        [ref_planes.ref_stack_id, ref_planes.ref_plane_id]),
        ForeignKeyConstraint(
            [stack_id], [stacks.stack_id]),
            {})

class modifications(Base):
    """docstring for modifications."""
    __tablename__ = TABLE_MODIFICATION
    modification_id = Column(Integer(), primary_key=True, autoincrement=True)
    modification_name = Column(String(200), unique=True)
    modification_prefix = Column(String(200), unique=True)

class stack_modifications(Base):
    """docstring for stack_modifications."""
    __tablename__ = TABLE_STACKMODIFICATION
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
            [modification_id],[modifications.modification_id]),
        {})

class object_filter_names(Base):
    __tablename__ = TABLE_FILTER_NAMES
    object_filter_id = Column(Integer(), primary_key=True, autoincrement=True)
    object_filter_name = Column(String(200), unique=True)


class object_filters(Base):
    __tablename__ = TABLE_FILTERS
    object_filter_id = Column(Integer(), primary_key=True)
    object_id =  Column(Integer(),
                       primary_key=True)
    filter_value = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [object_id],
        [objects.object_id]),
        ForeignKeyConstraint([object_filter_id], [object_filter_names.object_filter_id]), {})

class object_relations(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS
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
            [objects.object_id]),{})

class ObjectRelationsTypes(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS_TYPES
    object_relationtype_id = Column(Integer(), primary_key=True, autoincrement=True)
    object_relationtype_name = Column(String(200), index=True, unique=True)

class measurement_names(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_NAME
    measurement_name = Column(String(200), primary_key=True)

class measurement_types(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_TYPE
    measurement_type = Column(String(200), primary_key=True)

class measurements(Base):
    __tablename__ = TABLE_MEASUREMENT_META
    measurement_id = Column(Integer(), primary_key=True, autoincrement=True)
    measurement_type = Column(String(200))
    measurement_name = Column(String(200))
    plane_id = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [measurement_name], [measurement_names.measurement_name]),
        ForeignKeyConstraint([measurement_type], [measurement_types.measurement_type]),
        ForeignKeyConstraint([plane_id], [planes.plane_id]),
        UniqueConstraint(measurement_name, measurement_type, plane_id),{})

class object_measurements(Base):
    """docstring for object_measurements."""
    __tablename__ = TABLE_MEASUREMENT
    object_id = Column(Integer(),
                       primary_key=True)
    measurement_id = Column(Integer(), primary_key=True)
    value = Column(Float(precision=32))
    __table_args__ = (ForeignKeyConstraint(
        [object_id],
        [objects.object_id]),
        ForeignKeyConstraint(
            [measurement_id], [measurements.measurement_id])
        ,{})


TABLE_PANNEL = 'pannel'
PANNEL_KEY_METAL = 'Metal'
PANNEL_KEY_TARGET = 'Target'
PANNEL_COL_ABCLONE = 'AntibodyClone'
PANNEL_COL_CONCENTRATION = 'Concentration'
PANNEL_COL_ILASTIK = 'Ilastik'
PANNEL_COL_BARCODE = 'Barcode'
PANNEL_COL_TUBENUMBER = 'TubeNumber'

class pannel(Base):
    """docstring for pannel."""
    __tablename__ = TABLE_PANNEL
    Metal = Column(String(200), primary_key=True)
    Target = Column(String(200), primary_key=True)
    AntibodyClone  = Column(String(200))
    Concentration = Column(Float())
    Ilastik = Column(Boolean())
    Barcode = Column(Boolean())
    TubeNumber = Column(Integer())


class mask_measurements(Base):
    """docstring for image_measurements."""
    __tablename__=TABLE_IMAGEMEASUREMENT
    image_id = Column(Integer(), primary_key=True)
    object_type = Column(String(200),
                       primary_key=True)
    measurement_id = Column(String(200), primary_key=True)
    value = Column(Float())
    __table_args__ = (ForeignKeyConstraint(
        [image_id],
        [images.image_id]), ForeignKeyConstraint(
            [object_type], [masks.object_type]),
        ForeignKeyConstraint(
            [measurement_id], [measurements.measurement_id]),
        {})

class image_measurements(Base):
    """docstring for image_measurements."""
    __tablename__=TABLE_IMAGEMEASUREMENT
    image_id = Column(Integer(), primary_key=True)
    object_type = Column(String(200),
                       primary_key=True)
    measurement_id = Column(String(200), primary_key=True)
    value = Column(Float())
    __table_args__ = (ForeignKeyConstraint(
        [image_id],
        [images.image_id]), ForeignKeyConstraint(
            [object_type], [masks.object_type]),
        ForeignKeyConstraint(
            [measurement_id], [measurements.measurement_id]),
        {})
