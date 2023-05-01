import requests
import prefect
from prefect import task,Flow,flow
import psycopg2
import generic

@task
def get_user_in_game_data():
    sql  = '''
            select * from "public".dashboard_user_in_game_details
            '''
    data = generic.connect_to_web_database(sql)
    list_records = []
    for record in data:
        record_dict ={
            'user_id':record[4],
            'in_game_name': record[1],
            'tagline': record[2],
            'region': record[3],
            'valorant_puuid': record[5]    
        }
        list_records.append(record_dict)
    send_in_game_data_to_warehouse(list_records)
    return True

def send_in_game_data_to_warehouse(data):
    dups_check_query = '''
        SELECT user_id FROM raw.user_in_game_details
        WHERE 
            in_game_name = %s
            and tagline = %s
            and valorant_puuid = %s ;
    '''
    for record in data:
        user_id = record['user_id']
        in_game_name = record['in_game_name']
        tagline = record['tagline']
        region = record['region']
        valorant_puuid = record['valorant_puuid']
        if generic.dups_handler(dups_check_query,params=(in_game_name, tagline, valorant_puuid)):
            current_timestamp = generic.get_current_utc_timestamp()
            sql = ('''
                INSERT INTO "raw".user_in_game_details(user_id, in_game_name, tagline, region, valorant_puuid, _loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s);
            ''')
            values = (user_id, in_game_name, tagline, region, valorant_puuid, current_timestamp)
            generic.sql_execute(sql,values)
    return True

@task
def get_user_social_account():
    sql  = '''
            select * from "public".socialaccount_socialaccount
            '''
    data = generic.connect_to_web_database(sql)
    list_records = []
    for record in data:
        record_dict ={
            'user_id': record[6],
            'vendor': record[1],
            'date_joined': record[4],
            '_data': record[5]
        }
        list_records.append(record_dict)
    print(list_records[1])
    send_social_accounts_data_to_warehouse(list_records)
    return True

def send_social_accounts_data_to_warehouse(data):
    dups_check_query = '''
        SELECT user_id FROM raw.user_social_details
        WHERE 
            user_id = %s
            and vendor = %s
            and date_joined = %s ;
    '''
    for record in data:
        user_id = record['user_id']
        vendor = record['vendor']
        date_joined = record['date_joined']
        _data = record['_data']
        if generic.dups_handler(dups_check_query,params=(user_id, vendor, str(date_joined))):
            current_timestamp = generic.get_current_utc_timestamp()
            sql = ('''
                INSERT INTO "raw".user_social_details(user_id, vendor, date_joined, _data, _loaded_at )
                VALUES (%s, %s, %s, %s, %s);
            ''')
            values = (user_id, vendor, date_joined, _data, current_timestamp)
            generic.sql_execute(sql,values)
    return True


@flow(name = 'User Data Extract')
def pull_data():
    get_user_in_game_data()
    get_user_social_account()
    pass

pull_data()