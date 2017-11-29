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

KEY_CHANNEL_NAME = 'ChannelName'
KEY_CHANNEL_TYPE = 'ChannelType'
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


TABLE_CONDITION = 'Condition'
TABLE_SITE = 'Site'

TABLE_DERIVEDSTACK = 'DerivedStack'
TABLE_FILTERS = 'Filters'
TABLE_FILTER_NAMES = 'FilterNames'
TABLE_IMAGE = 'Image'
TABLE_MASKS = 'Masks'
TABLE_MEASUREMENT = 'Measurement'
TABLE_MEASUREMENT_META = 'MeasurementMeta'
TABLE_MEASUREMENT_NAME = 'MeasurementNames'
TABLE_MEASUREMENT_TYPE = 'MeasurementTypes'
TABLE_MODIFICATION = 'Modification'
TABLE_OBJECT = 'Objects'
TABLE_OBJECT_RELATIONS = 'ObjectRelations'
TABLE_OBJECT_RELATIONS_TYPES = 'ObjectRelationsTypes'
TABLE_PLANEMETA = 'PlaneMeta'
TABLE_REFPLANEMETA = 'RefPlaneMeta'
TABLE_REFSTACK = 'RefStack'
TABLE_STACK = 'Stack'
TABLE_STACKMODIFICATION = 'StackModification'
TABLE_IMAGEMEASUREMENT = 'ImageMeasurement'


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
class Condition(Base):
    """docstring for Image."""
    __tablename__ = TABLE_CONDITION
    ConditionID = Column(String(200), primary_key=True)
    ConditionName = Column(String(200), server_default='default')
    TimePoint = Column(Float(), server_default="0")
    BarCode = Column(String(200))
    Concentration = Column(Float())
    PlateID = Column(Integer())
    BCPlate = Column(Integer())
    BCX = Column(Integer())
    BCY = Column(String(200))

class Site(Base):
    """docstring for Image."""
    __tablename__ = TABLE_SITE
    SiteName = Column(String(200), primary_key=True)



class Image(Base):
    """docstring for Image."""
    __tablename__ = TABLE_IMAGE
    ImageNumber = Column(Integer(), primary_key=True)
    BCDepth = Column(Float())
    BCInvalid = Column(Integer())
    BCValid = Column(Integer())
    BCHighestCount = Column(Integer())
    BCSecondCount = Column(Integer())
    ConditionID = Column(String(200))
    SiteName = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
        [ConditionID],
        [Condition.ConditionID]),
        ForeignKeyConstraint(
        [SiteName],
        [Site.SiteName]),
            {})

class Masks(Base):
    """ a table describing the masks."""
    __tablename__ = TABLE_MASKS
    ObjectID = Column(String(200),
                       primary_key=True)
    ImageNumber = Column(Integer(),  primary_key=True)
    PosX = Column(Integer())
    PosY = Column(Integer())
    ShapeH = Column(Integer())
    ShapeW = Column(Integer())
    CropID = Column(Integer())
    FileName = Column(String(200))

class Objects(Base):
    """docstring for Objects."""
    __tablename__ = TABLE_OBJECT
    ObjectNumber = Column(Integer())
    ObjectUniID = Column(Integer(), primary_key=True)
    ImageNumber = Column(Integer(), index=True)
    ObjectID = Column(String(200), index=True)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber],
        [Image.ImageNumber]),
        ForeignKeyConstraint(
        [ObjectID, ImageNumber],
        [Masks.ObjectID, Masks.ImageNumber]),{})

class RefStack(Base):
    """docstring for RefStack."""
    __tablename__ = TABLE_REFSTACK
    RefStackID = Column(Integer(), primary_key=True)
    RefStackName = Column(String(200), unique=True)
    Scale = Column(Float())

class RefPlaneMeta(Base):
    """docstring for PlaneMeta."""
    __tablename__ = TABLE_REFPLANEMETA
    RefStackID = Column(Integer(), primary_key=True)
    PlaneID = Column(Integer(), primary_key=True)
    ChannelType = Column(String(200))
    ChannelName = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [RefStackID],
        [RefStack.RefStackID]),{})

class Stack(Base):
    """docstring for Stack."""
    __tablename__ = TABLE_STACK
    StackID = Column(Integer(), primary_key=True)
    StackName = Column(String(200), unique=True)
    RefStackID = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [RefStackID], [RefStack.RefStackID]), {})

class PlaneMeta(Base):
    __tablename__ = TABLE_PLANEMETA
    PlaneUniID = Column(Integer(), primary_key=True)
    StackID = Column(Integer())
    PlaneID = Column(Integer())
    RefStackID = Column(Integer())
    __table_args__ = (
        ForeignKeyConstraint(
        [RefStackID, PlaneID],
        [RefPlaneMeta.RefStackID, RefPlaneMeta.PlaneID]),
        ForeignKeyConstraint(
            [StackID], [Stack.StackID]),
            {})

class Modification(Base):
    """docstring for Modification."""
    __tablename__ = TABLE_MODIFICATION
    ModificationID = Column(Integer(), primary_key=True)
    ModificationName = Column(String(200), unique=True)
    ModificationPrefix = Column(String(200), unique=True)

class StackModification(Base):
    """docstring for StackModification."""
    __tablename__ = TABLE_STACKMODIFICATION
    ModificationID = Column(Integer(),
                              primary_key=True)
    ParentID = Column(Integer(), primary_key=True)
    ChildID = Column(Integer(), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
        [ParentID],
        [Stack.StackID]),
        ForeignKeyConstraint(
        [ChildID],
        [Stack.StackID]),
        ForeignKeyConstraint(
            [ModificationID],[Modification.ModificationID]),
        {})

class FilterNames(Base):
    __tablename__ = TABLE_FILTER_NAMES
    FilterID = Column(Integer(), primary_key=True)
    FilterName = Column(String(200), unique=True)


class Filters(Base):
    __tablename__ = TABLE_FILTERS
    FilterID = Column(Integer(), primary_key=True)
    ObjectUniID =  Column(Integer(),
                       primary_key=True)
    FilterValue = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [ObjectUniID],
        [Objects.ObjectUniID]),
        ForeignKeyConstraint([FilterID], [FilterNames.FilterID]), {})

class ObjectRelations(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS
    ObjectUniIDFrom = Column(Integer(),
                       primary_key=True)
    ObjectUniIDTo = Column(Integer(),
                       primary_key=True)
    RelationshipID = Column(Integer(), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [ObjectUniIDFrom],
        [Objects.ObjectUniID]),
        ForeignKeyConstraint(
        [ObjectUniIDTo],
            [Objects.ObjectUniID]),{})

class ObjectRelationsTypes(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS_TYPES
    RelationshipID = Column(Integer(), primary_key=True)
    RelationshipName = Column(String(200), index=True, unique=True)

class MeasurementNames(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_NAME
    MeasurementName = Column(String(200), primary_key=True)

class MeasurementTypes(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_TYPE
    MeasurementType = Column(String(200), primary_key=True)

class MeasurementMeta(Base):
    __tablename__ = TABLE_MEASUREMENT_META
    MeasurementID = Column(Integer(), primary_key=True)
    MeasurementType = Column(String(200))
    MeasurementName = Column(String(200))
    PlaneUniID = Column(Integer())
    __table_args__ = (ForeignKeyConstraint(
        [MeasurementName], [MeasurementNames.MeasurementName]),
        ForeignKeyConstraint([MeasurementType], [MeasurementTypes.MeasurementType]),
        ForeignKeyConstraint([PlaneUniID], [PlaneMeta.PlaneUniID]),
        UniqueConstraint(MeasurementName, MeasurementType, PlaneUniID),{})

class Measurement(Base):
    """docstring for Measurement."""
    __tablename__ = TABLE_MEASUREMENT
    ObjectUniID = Column(Integer(),
                       primary_key=True)
    MeasurementID = Column(Integer(), primary_key=True)
    Value = Column(Float(precision=32))
    __table_args__ = (ForeignKeyConstraint(
        [ObjectUniID],
        [Objects.ObjectUniID]),
        ForeignKeyConstraint(
            [MeasurementID], [MeasurementMeta.MeasurementID])
        ,{})


TABLE_PANNEL = 'Pannel'
PANNEL_KEY_METAL = 'Metal'
PANNEL_KEY_TARGET = 'Target'
PANNEL_COL_ABCLONE = 'AntibodyClone'
PANNEL_COL_CONCENTRATION = 'Concentration'
PANNEL_COL_ILASTIK = 'Ilastik'
PANNEL_COL_BARCODE = 'Barcode'
PANNEL_COL_TUBENUMBER = 'TubeNumber'

class Pannel(Base):
    """docstring for Pannel."""
    __tablename__ = TABLE_PANNEL
    Metal = Column(String(200), primary_key=True)
    Target = Column(String(200), primary_key=True)
    AntibodyClone  = Column(String(200))
    Concentration = Column(Float())
    Ilastik = Column(Boolean())
    Barcode = Column(Boolean())
    TubeNumber = Column(Integer())


class ImageMeasurement(Base):
    """docstring for ImageMeasurement."""
    __tablename__=TABLE_IMAGEMEASUREMENT
    ImageNumber = Column(Integer(), primary_key=True)
    ObjectID = Column(String(200),
                       primary_key=True)
    MeasurementName = Column(String(200), primary_key=True)
    Value = Column(Float())
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber],
        [Image.ImageNumber])
        ,{})
