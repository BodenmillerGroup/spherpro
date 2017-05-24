# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float
Base = declarative_base()


def connect_sqlite(conf):
    """
    creates a sqlite connector to be used with the Datastore.

    Args:
        conf: the config dictionnary from a Datastore object.

    Returns:
        SQLite3 connector
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
    host=conf['mysql']['host']
    port=conf['mysql'].get('port', '3306')
    user=conf['mysql']['user']
    password=conf['mysql']['pass']
    db=conf['mysql']['db']
    conn = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, password, host, port, db)
    engine = create_engine(conn)
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



class Image(Base):
    """docstring for Image."""
    __tablename__ = 'Image'
    ImageNumber = Column(Integer, primary_key=True)

class Cell(Base):
    """docstring for Cell."""
    __tablename__ = 'Cell'
    CellNumber = Column(Integer, primary_key=True)
    ImageNumber = Column(Integer, ForeignKey(Image.ImageNumber), primary_key=True)

class Stack(Base):
    """docstring for Stack."""
    __tablename__ = 'Stack'
    StackName = Column(String(200), primary_key=True)

class Measurement(Base):
    """docstring for Measurement."""
    __tablename__ = 'Measurement'
    ImageNumber = Column(Integer, ForeignKey(Image.ImageNumber), primary_key=True)
    CellNumber = Column(Integer, ForeignKey(Cell.CellNumber), primary_key=True)
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
    Name = Column(String(222))
    Type = Column(String(222))

class Modification(Base):
    """docstring for Modification."""
    __tablename__ = 'Modification'
    ModificationName = Column(String(200), primary_key=True)
    ModificationPrefix = Column(String(222))

class StackModification(Base):
    """docstring for StackModification."""
    __tablename__ = 'StackModification'
    ModificationName = Column(String(200), ForeignKey(Modification.ModificationName), primary_key=True)
    ParentName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)
    ChildName = Column(String(200), ForeignKey(Stack.StackName), primary_key=True)
