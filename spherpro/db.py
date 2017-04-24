# Diffrent database collectors used by the datastore
import sqlite3
import pymysql
from sqlalchemy import create_engine

def connect_sqlite(conf):
    return sqlite3.connect(conf['sqlite']['db'])

def connect_mysql(conf):
    host=conf['mysql']['host']
    port=conf['mysql'].get('port', '3306')
    user=conf['mysql']['user']
    password=conf['mysql']['pass']
    db=conf['mysql']['db']
    conn = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, password, host, port, db)
    return create_engine(conn)