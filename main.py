import requests
from prefect import flow,task
import psycopg2

from datetime import timezone
import datetime
   
def get_current_utc_timestamp():
    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    return utc_time

def get_player_details():
    try:
        connection = psycopg2.connect(database="Web",
                                host="db.yzqgftlyckkypgpmyjig.supabase.co",
                                user="postgres",
                                password="d7d8xAQuj7GSDvst",
                                port="5432")
        cursor = connection.cursor()
        postgreSQL_select_Query = '''
            select * from "public".dashboard_user_in_game_details
            '''
        cursor.execute(postgreSQL_select_Query)
        print("Selecting rows from mobile table using cursor.fetchall")
        player_records = cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            return player_records

def get_conn_details():
    conn = psycopg2.connect(database="DataWarehouse",
                            host="db.yzqgftlyckkypgpmyjig.supabase.co",
                            user="postgres",
                            password="d7d8xAQuj7GSDvst",
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
    
def metadata_dups_handler(value):
    cursor = establish_conn()
    sql = ('''
        SELECT match_id FROM raw.metadata WHERE match_id = %s;
    ''')
    cursor.execute(sql,[value])
    data = cursor.fetchone()
    if data is None:
        print('No value in table')
        return True
    else:
        print('Found in table')
        return False


@task(retries=3,retry_delay_seconds=60)
def get_match_details(region,name,tag,url):
    modified_url = str(url)+str(region)+'/'+str(name)+'/'+str(tag)
    response = requests.get(modified_url)
    return response

@task
def get_match_data(response):
    length = len(response.json()['data'])
    listx = []
    for i in range(length):
        response_data = response.json()['data'][i]
        listx.append(response_data)    
    return listx

@task
def filtered_data(response_data, puuid):
    current_timestamp = str(get_current_utc_timestamp())
    # print(response_data['metadata'])
    match_id = str(response_data['metadata']['matchid'])
    played_map = str(response_data['metadata']['map'])
    start_at = str(response_data['metadata']['game_start'])
    duration = str(response_data['metadata']['game_length'])
    mode = str(response_data['metadata']['mode'])
    season_id = str(response_data['metadata']['season_id'])
    cluster = str(response_data['metadata']['cluster'])
    rounds_played = str(response_data['metadata']['rounds_played'])
    players_data = str(response_data['players'])
    teams_data = str(response_data['teams'])
    print(teams_data)
    sql = ('''
        INSERT INTO "raw".metadata(match_id, map, start_at, duration, mode, season_id, cluster, rounds_played, _loaded_at, players_data, puuid, teams)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''')
    values = (match_id, played_map, start_at, duration, mode, season_id, cluster, rounds_played, current_timestamp, players_data, puuid, teams_data)
    sql_execute(sql,values)
    return True

@flow
def get_match_flow(region,name,tag,url,puuid):
    response=get_match_details(region,name,tag,url)
    match_data_response = get_match_data(response)
    for match in match_data_response:
        match_id = str(match['metadata']['matchid'])
        print(type(match_id))

        if metadata_dups_handler(match_id):
            filtered_data(match,puuid)
        else:
            pass
    return True

@flow
def pull_data():
    result = get_player_details()
    url = 'https://api.henrikdev.xyz/valorant/v3/matches/'
    for player_info in result:
        print(player_info)
        name = player_info[1]
        tag = player_info[2]
        region = player_info[3]
        puuid = player_info[5]
        get_match_flow(region,name,tag,url,puuid)
    print('pulled')

pull_data()