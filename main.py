import requests
from prefect import task,flow
import generic
from datetime import timezone
import os

api_key = os.environ.get('API_KEY')

@task
def get_player_details():
    sql ='''
        select * from "public".dashboard_user_in_game_details
        '''
    data = generic.connect_to_web_database(sql)
    return data

@task(retries=3,retry_delay_seconds=60)
def get_match_details(region,name,tag,url):
    modified_url = str(url)+str(region)+'/'+str(name)+'/'+str(tag)
    response = requests.get(modified_url ,headers={'Authorization': str(api_key)})
    print(response.status_code)
    print('\n')
    print('\n')
    print('\n')
    print('\n')
    print('\n')

    print('\n')
    print('\n')
    print('\n')
    print('\n')
    return response

@task
def get_match_data_in_list(response):
    length = len(response.json()['data'])
    listx = []
    for i in range(length):
        response_data = response.json()['data'][i]
        listx.append(response_data)
    return listx

@task
def transformation(response_data, puuid):
    current_timestamp = str(generic.get_current_utc_timestamp())
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
    result = (match_id, played_map, start_at, duration, mode, season_id, cluster, rounds_played, current_timestamp, players_data, puuid, teams_data)
    return result
    
@task
def load_into_warehouse(values):
    sql = ('''
        INSERT INTO "raw".metadata(match_id, map, start_at, duration, mode, season_id, cluster, rounds_played, _loaded_at, players_data, puuid, teams)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''')
    generic.sql_execute(sql,values)
    return True

@flow
def transform_and_load(result):
    url = 'https://api.henrikdev.xyz/valorant/v3/matches/'
    dups_check_query = '''
        SELECT match_id FROM raw.metadata WHERE match_id = %s ;
    '''
    if result is None:
        for player_info in result:
            name = player_info[1]
            tag = player_info[2]
            region = player_info[3]
            puuid = player_info[5]

            response=get_match_details(region,name,tag,url)

            if response.json()['status'] == 200:
                match_data_response = get_match_data_in_list(response)
                for match in match_data_response:
                    match_id = str(match['metadata']['matchid'])
                    if generic.dups_handler(dups_check_query, params = (match_id, )):
                        transformed_data = transformation(match,puuid)
                        load_into_warehouse(transformed_data)
                    else:
                        pass
            else:
                pass
    return True


@flow(name = 'Valorant_Data_Extract')
def pull_data():
    result = get_player_details()
    transform_and_load(result)

pull_data()


