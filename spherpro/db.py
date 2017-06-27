# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Boolean,\
     ForeignKeyConstraint
Base = declarative_base()

# Define the table and column names to be used
# These need to match the definitions bellow

KEY_CHANNEL_NAME = 'ChannelName'
KEY_CHANNEL_TYPE = 'ChannelType'
KEY_CHILDNAME = 'ChildName'
KEY_DISPLAY_NAME = 'DisplayName'
KEY_FILENAME = 'FileName'
KEY_IMAGENUMBER = 'ImageNumber'
KEY_IMAGENUMBER_FROM = 'ImageNumberFrom'
KEY_IMAGENUMBER_TO = 'ImageNumberTo'
KEY_MEASUREMENTNAME = 'MeasurementName'
KEY_MEASUREMENTTYPE = 'MeasurementType'
KEY_MODIFICATIONNAME = 'ModificationName'
KEY_MODIFICATIONPREFIX = 'ModificationPrefix'
KEY_OBJECTID = 'ObjectID'
KEY_OBJECTID_FROM = 'ObjectIDFrom'
KEY_OBJECTID_TO = 'ObjectIDTo'
KEY_OBJECTNUMBER = 'ObjectNumber'
KEY_OBJECTNUMBER_FROM = 'ObjectNumberFrom'
KEY_OBJECTNUMBER_TO = 'ObjectNumberTo'
KEY_PARENTNAME = 'ParentName'
KEY_PLANEID = 'PlaneID'
KEY_REFSTACKNAME = 'RefStackName'
KEY_RELATIONSHIP = 'Relationship'
KEY_SCALE = 'Scale'
KEY_STACKNAME = 'StackName'
KEY_VALUE = 'Value'
KEY_CROPID = 'CropID'
KEY_POSX = 'PosX'
KEY_POSY = 'PosY'


KEY_FILTERNAME = 'FilterName'
KEY_FILTERVALUE = 'FilterValue'

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
TABLE_IMAGE = 'Image'
TABLE_MASKS = 'Masks'
TABLE_MEASUREMENT = 'Measurement'
TABLE_MEASUREMENT_NAME = 'MeasurementName'
TABLE_MEASUREMENT_TYPE = 'MeasurementType'
TABLE_MODIFICATION = 'Modification'
TABLE_OBJECT = 'Objects'
TABLE_OBJECT_RELATIONS = 'ObjectRelations'
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
    __table_args__ = (
        ForeignKeyConstraint(
        [ConditionID],
        [Condition.ConditionID]),
            {})

class Masks(Base):
    """ a table describing the masks."""
    __tablename__ = TABLE_MASKS
    ObjectID = Column(String(200),
                       primary_key=True)
    ImageNumber = Column(Integer(),  primary_key=True)
    PosX = Column(Integer())
    PosY = Column(Integer())
    CropID = Column(Integer())
    FileName = Column(String(200))

class Objects(Base):
    """docstring for Objects."""
    __tablename__ = TABLE_OBJECT
    ObjectNumber = Column(Integer(), primary_key=True)
    ImageNumber = Column(Integer(), primary_key=True)
    ObjectID = Column(String(200), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber],
        [Image.ImageNumber]),
        ForeignKeyConstraint(
        [ObjectID, ImageNumber],
        [Masks.ObjectID, Masks.ImageNumber]),{})

class RefStack(Base):
    """docstring for RefStack."""
    __tablename__ = TABLE_REFSTACK
    RefStackName = Column(String(200), primary_key=True)
    Scale = Column(Float())

class RefPlaneMeta(Base):
    """docstring for PlaneMeta."""
    __tablename__ = TABLE_REFPLANEMETA
    RefStackName = Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    ChannelType = Column(String(200))
    ChannelName = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [RefStackName],
        [RefStack.RefStackName]),{})

class Stack(Base):
    """docstring for Stack."""
    __tablename__ = TABLE_STACK
    StackName = Column(String(200), primary_key=True)
    RefStackName = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [RefStackName], [RefStack.RefStackName]), {})

class PlaneMeta(Base):
    __tablename__ = TABLE_PLANEMETA
    StackName = Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    RefStackName = Column(String(200))
    __table_args__ = (
        ForeignKeyConstraint(
        [RefStackName, PlaneID],
        [RefPlaneMeta.RefStackName, RefPlaneMeta.PlaneID]),
        ForeignKeyConstraint(
            [StackName], [Stack.StackName]),
            {})

class Modification(Base):
    """docstring for Modification."""
    __tablename__ = TABLE_MODIFICATION
    ModificationName = Column(String(200), primary_key=True)
    ModificationPrefix = Column(String(200))

class StackModification(Base):
    """docstring for StackModification."""
    __tablename__ = TABLE_STACKMODIFICATION
    ModificationName = Column(String(200),
                              primary_key=True)
    ParentName = Column(String(200), primary_key=True)
    ChildName = Column(String(200), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
        [ParentName],
        [Stack.StackName]),
        ForeignKeyConstraint(
        [ChildName],
        [Stack.StackName]),
        ForeignKeyConstraint(
            [ModificationName],[Modification.ModificationName]),
        {})

class Filters(Base):
    __tablename__ = TABLE_FILTERS
    FilterName = Column(String(200), primary_key=True)
    ImageNumber = Column(Integer(),
                         primary_key=True)
    ObjectNumber = Column(Integer(),
                          primary_key=True)
    ObjectID =  Column(String(200),
                       primary_key=True)
    FilterValue = Column(Boolean(), primary_key=False)
    __table_args__ = (ForeignKeyConstraint(
        [ObjectNumber, ImageNumber, ObjectID],
        [Objects.ObjectNumber, Objects.ImageNumber, Objects.ObjectID]), {})

class ObjectRelations(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS
    ImageNumberFrom = Column(Integer(),
                         primary_key=True)
    ObjectNumberFrom = Column(Integer(),
                          primary_key=True)
    ObjectIDFrom = Column(String(200),
                       primary_key=True)
    ImageNumberTo = Column(Integer(),
                         primary_key=True)
    ObjectNumberTo = Column(Integer(),
                          primary_key=True)
    ObjectIDTo = Column(String(200),
                       primary_key=True)
    Relationship = Column(String(200), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumberFrom, ObjectNumberFrom, ObjectIDFrom],
        [Objects.ImageNumber, Objects.ObjectNumber, Objects.ObjectID]),
        ForeignKeyConstraint(
        [ImageNumberTo, ObjectNumberTo, ObjectIDTo],
            [Objects.ImageNumber, Objects.ObjectNumber, Objects.ObjectID]),{})

class Measurement(Base):
    """docstring for Measurement."""
    __tablename__ = TABLE_MEASUREMENT
    ImageNumber = Column(Integer(), primary_key=True)
    ObjectNumber = Column(Integer(),  primary_key=True)
    ObjectID = Column(String(200),
                       primary_key=True)
    MeasurementType = Column(String(200), primary_key=True)
    MeasurementName = Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    StackName = Column(String(200), primary_key=True)
    Value = Column(Float())
    __table_args__ = (ForeignKeyConstraint(
        [ObjectNumber, ImageNumber, ObjectID],
        [Objects.ObjectNumber, Objects.ImageNumber, Objects.ObjectID]),
        ForeignKeyConstraint(
            [StackName, PlaneID],
            [PlaneMeta.StackName, PlaneMeta.PlaneID])
        ,{})

class MeasurementName(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_NAME
    MeasurementName = Column(String(200), primary_key=True)

class MeasurementType(Base):
    """
    Convenience table
    """
    __tablename__ = TABLE_MEASUREMENT_TYPE
    MeasurementType = Column(String(200), primary_key=True)


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
