#!/usr/bin/python
import psycopg2
from config import config
 

conn = None

def connect():
    """ Connect to the PostgreSQL database server """
    try:
        global conn
        params = config()
        conn = psycopg2.connect(**params)
        if conn is not None:
            print('connection done')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def closeConnection():
    if conn is not None:
        conn.close()
        print('Database connection closed.')

def executeQuerry(querry):
    try:
        cur = conn.cursor()
        cur.execute(querry)
        result=cur.fetchall()
        cur.close()
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)