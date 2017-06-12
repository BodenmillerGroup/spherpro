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

KEY_IMAGENUMBER = 'ImageNumber'
KEY_OBJECTNUMBER = 'ObjectNumber'
KEY_MEASUREMENTTYPE = 'MeasurementType'
KEY_MEASUREMENTNAME = 'MeasurementName'
KEY_STACKNAME = 'StackName'
KEY_PLANEID = 'PlaneID'
KEY_CHANNEL_NAME = 'ChannelName'
KEY_DISPLAY_NAME = 'DisplayName'
KEY_CHANNEL_TYPE = 'ChannelType'
KEY_REFSTACKNAME = 'RefStackName'
KEY_OBJECTID = 'ObjectID'
KEY_FILENAME = 'FileName'
KEY_IMAGENUMBER_FROM = 'ImageNumberFrom'
KEY_IMAGENUMBER_TO = 'ImageNumberTo'
KEY_OBJECTNUMBER_FROM = 'ObjectNumberFrom'
KEY_OBJECTNUMBER_TO = 'ObjectIDTo'
KEY_OBJECTID_FROM = 'ObjectIDFrom'
KEY_OBJECTID_TO = 'ObjectIDTo'
KEY_RELATIONSHIP = 'Relationship'
KEY_SCALE = 'Scale'
KEY_CHILDNAME = 'ChildName'
KEY_MODIFICATIONNAME = 'ModificationName'
KEY_PARENTNAME = 'ParentName'

TABLE_MEASUREMENT = 'Measurement'
TABLE_IMAGE = 'Image'
TABLE_OBJECT = 'Objects'
TABLE_MODIFICATION = 'Modification'
TABLE_MEASUREMENT_NAME = 'MeasurementName'
TABLE_MEASUREMENT_TYPE = 'MeasurementType'
TABLE_MASKS = 'Masks'
TABLE_FILTERS = 'Filters'
TABLE_OBJECT_RELATIONS = 'ObjectRelations'
TABLE_PLANES = 'PlaneMeta'
TABLE_REFSTACK = 'RefStack'
TABLE_DERIVEDSTACK = 'DerivedStack'

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
    #Base.metadata.create_all(engine)
    return engine


################################################################################
#                           Model Definitions                                  #
################################################################################

class Image(Base):
    """docstring for Image."""
    __tablename__ = 'Image'
    ImageNumber = Column(Integer, primary_key=True)
    Scale = Column(Float)

class Objects(Base):
    """docstring for Objects."""
    __tablename__ = 'Objects'
    ObjectNumber = Column(Integer, primary_key=True)
    ImageNumber = Column(Integer, primary_key=True)
    ObjectID = Column(String(200), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber],
        [Image.ImageNumber]),{})


class RefStack(Base):
    """docstring for RefStack."""
    __tablename__ = TABLE_REFSTACK
    RefStackName = Column(String(200), primary_key=True)
    Scale = Column(Float)

class PlaneMeta(Base):
    """docstring for PlaneMeta."""
    __tablename__ = TABLE_PLANES
    RefStackName = Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    ChannelType = Column(String(200))
    ChannelName = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [RefStackName],
        [RefStack.RefStackName]),{})

class DerivedStack(Base):
    """docstring for DerivedStack."""
    __tablename__ = TABLE_DERIVEDSTACK
    StackName = Column(String(200), primary_key=True)
    RefStackName = Column(String(200), primary_key=True)
    __table_args__ =(ForeignKeyConstraint(
        [RefStackName],[RefStack.RefStackName]),{})


class Stack(Base):
    """docstring for Stack."""
    __tablename__ = 'Stack'
    StackName = Column(String(200), primary_key=True)
    __table_args__ = (ForeignKeyConstraint(
        [StackName], [DerivedStack.StackName]), {})

class Masks(Base):
    """ a table describing the masks."""
    __tablename__ = 'Masks'
    ImageNumber = Column(Integer,  primary_key=True)
    ObjectID = Column(String(200),
                       primary_key=True)
    FileName = Column(String(200))
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber, ObjectID],
        [Objects.ImageNumber, Objects.ObjectID]),{})

class Modification(Base):
    """docstring for Modification."""
    __tablename__ = 'Modification'
    ModificationName = Column(String(200), primary_key=True)
    ModificationPrefix = Column(String(200))

class StackModification(Base):
    """docstring for StackModification."""
    __tablename__ = 'StackModification'
    ModificationName = Column(String(200),
                              primary_key=True)
    ParentName = Column(String(200), primary_key=True)
    ChildName = Column(String(200), primary_key=True)
    __table_args__ = (
        ForeignKeyConstraint(
        [ParentName],
        [DerivedStack.StackName]),
        ForeignKeyConstraint(
        [ChildName],
        [DerivedStack.StackName]),
        ForeignKeyConstraint(
            [ModificationName],[Modification.ModificationName]))

class Filters(Base):
    __tablename__ = TABLE_FILTERS
    FilterName = Column(String(200), primary_key=True)
    FilterValue = Column(Boolean(), primary_key=True)
    ImageNumber = Column(Integer,
                         primary_key=False)
    ObjectNumber = Column(Integer,
                          primary_key=False)
    ObjectID =  Column(String(200),
                       primary_key=False)
    __table_args__ = (ForeignKeyConstraint(
        [ObjectNumber, ObjectID, ImageNumber],
        [Objects.ObjectNumber, Objects.ObjectID, Objects.ImageNumber]), {})

class ObjectRelations(Base):
    __tablename__ = TABLE_OBJECT_RELATIONS
    ImageNumberFrom = Column(Integer, ForeignKey(Image.ImageNumber),
                         primary_key=True)
    ObjectNumberFrom = Column(Integer, ForeignKey(Objects.ObjectNumber),
                          primary_key=True)
    ObjectIDFrom = Column(String(200), ForeignKey(Objects.ObjectID),
                       primary_key=True)
    ImageNumberTo = Column(Integer, ForeignKey(Image.ImageNumber),
                         primary_key=False)
    ObjectNumberTo = Column(Integer, ForeignKey(Objects.ObjectNumber),
                          primary_key=False)
    ObjectIDTo = Column(String(200), ForeignKey(Objects.ObjectID),
                       primary_key=False)
    Relationship = Column(String(200), primary_key=False)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumberFrom, ObjectNumberFrom, ObjectIDFrom],
        [Objects.ImageNumber, Objects.ObjectNumber, Objects.ObjectID]),
        ForeignKeyConstraint(
        [ImageNumberTo, ObjectNumberTo, ObjectIDTo],
            [Objects.ImageNumber, Objects.ObjectNumber, Objects.ObjectID]))

class Measurement(Base):
    """docstring for Measurement."""
    __tablename__ = 'Measurement'
    ImageNumber = Column(Integer, primary_key=True)
    ObjectNumber = Column(Integer,  primary_key=True)
    ObjectID = Column(String(200),
                       primary_key=True)
    MeasurementType = Column(String(200), primary_key=True)
    MeasurementName = Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    StackName = Column(String(200), primary_key=True)
    Value = Column(Float)
    __table_args__ = (ForeignKeyConstraint(
        [ImageNumber, ObjectNumber, ObjectID],
        [Objects.ImageNumber, Objects.ObjectNumber, Objects.ObjectID]),
        ForeignKeyConstraint(
            [StackName],
         [Stack.StackName]),
        ForeignKeyConstraint([PlaneID],[PlaneMeta.PlaneID]))
