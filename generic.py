import requests
import prefect
from prefect import task,Flow,flow
import psycopg2

from datetime import timezone
import datetime
import os
   
# password = 'd7d8xAQuj7GSDvst'
password = str(os.environ.get('PASSWORD'))
# host_name = 'db.yzqgftlyckkypgpmyjig.supabase.co'
host_name = str(os.environ.get('HOST_NAME'))


def get_current_utc_timestamp():
    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    return utc_time

#functions for conencting to the warehouse
def get_conn_details():
    conn = psycopg2.connect(database="DataWarehouse",
                            host= host_name,
                            user= "postgres",
                            password= password,
                            port="5432")
    conn.autocommit = True
    return conn

def establish_conn():
    conn = get_conn_details()
    cursor = conn.cursor()
    return cursor

def sql_execute(sql,values):
    cursor = establish_conn()
    try:
        cursor.execute(sql,values)
        conn_commit()
    except Exception as error:
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))

def conn_commit():
    conn = get_conn_details()
    conn.commit()

def conn_terminate():
    conn = get_conn_details()
    conn.close()

#function to check if duplicate data present in raw database in warehouse
def dups_handler(dups_check_query,params):
    cursor = establish_conn()
    sql = (dups_check_query)
    cursor.execute(sql, params)
    data = cursor.fetchone()
    if data is None:
        return True
    else:
        return False

#functions to connect to web database
def connect_to_web_database(sql):
    player_records = []
    connection = False
    try:
        connection = psycopg2.connect(database="Web",
                                host= host_name,
                                user="postgres",
                                password= password,
                                port="5432")
        cursor = connection.cursor()
        postgreSQL_select_Query = sql
        cursor.execute(postgreSQL_select_Query)
        player_records = cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            return player_records
        
def connect_to_raw_database(sql):
    records = []
    connection = False
    try:
        connection = psycopg2.connect(database="DataWarehouse",
                                host= host_name,
                                user="postgres",
                                password= password,
                                port="5432")
        cursor = connection.cursor()
        postgreSQL_select_Query = sql
        cursor.execute(postgreSQL_select_Query)
        records = cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            return records