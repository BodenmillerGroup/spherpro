# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
from sqlalchemy import create_engine

def connect_sqlite(conf):
    """
    creates a sqlite connector to be used with the Datastore.

    Args:
        conf: the config dictionnary from a Datastore object.

    Returns:
        SQLite3 connector
    """
    return sqlite3.connect(conf['sqlite']['db'])

def connect_mysql(conf):
    """
    creates a MySQL connector to be used with the Datastore.

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
    return create_engine(conn)