# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float
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

TABLE_MEASUREMENT = 'Measurement'
TABLE_IMAGE = 'Image'
TABLE_OBJECT = 'Objects'
TABLE_MODIFICATION = 'Modification'
TABLE_MEASUREMENT_NAME = 'MeasurementName'
TABLE_MEASUREMENT_TYPE = 'MeasurementType'
TABLE_MASKS = 'Masks'

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

class Objects(Base):
    """docstring for Objects."""
    __tablename__ = 'Objects'
    ObjectNumber = Column(Integer, primary_key=True)
    ImageNumber = Column(Integer, ForeignKey(Image.ImageNumber), primary_key=True)
    ObjectID = Column(String(200), primary_key=True)

class Stack(Base):
    """docstring for Stack."""
    __tablename__ = 'Stack'
    StackName = Column(String(200), primary_key=True)

class Masks(Base):
    """ a table describing the masks."""
    __tablename__ = 'Masks'
    ImageNumber = Column(Integer, ForeignKey(Image.ImageNumber), primary_key=True)
    ObjectID = Column(String(200), ForeignKey(Objects.ObjectID),
                       primary_key=True)
    FileName = Column(String(200))


class Measurement(Base):
    """docstring for Measurement."""
    __tablename__ = 'Measurement'
    ImageNumber = Column(Integer, ForeignKey(Image.ImageNumber), primary_key=True)
    ObjectNumber = Column(Integer, ForeignKey(Objects.ObjectNumber), primary_key=True)
    ObjectID =  Column(String(200), ForeignKey(Objects.ObjectID),
                       primary_key=True)
    StackName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)
    MeasurementType =  Column(String(200), primary_key=True)
    MeasurementName =  Column(String(200), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    Value = Column(Float)


class RefStack(Base):
    """docstring for RefStack."""
    __tablename__ = 'RefStack'
    StackName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)

class DerivedStack(Base):
    """docstring for DerivedStack."""
    __tablename__ = 'DerivedStack'
    StackName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)
    RefStackName = Column(String(200), ForeignKey(RefStack.StackName), primary_key=True)


class PlaneMeta(Base):
    """docstring for PlaneMeta."""
    __tablename__ = 'PlaneMeta'
    RefStackName = Column(String(200), ForeignKey(RefStack.StackName), primary_key=True)
    PlaneID = Column(String(200), primary_key=True)
    ChannelType = Column(String(200))
    ChannelName = Column(String(200))

class Modification(Base):
    """docstring for Modification."""
    __tablename__ = 'Modification'
    ModificationName = Column(String(200), primary_key=True)
    ModificationPrefix = Column(String(200))

class StackModification(Base):
    """docstring for StackModification."""
    __tablename__ = 'StackModification'
    ModificationName = Column(String(200),
                              ForeignKey(Modification.ModificationName), primary_key=True)
    ParentName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)
    ChildName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)

