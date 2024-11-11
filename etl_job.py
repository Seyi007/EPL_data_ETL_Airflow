import requests
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pyodbc

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for football data',
    schedule_interval=timedelta(days=1),
)


league_url = 'https://apiv2.apifootball.com/?action=get_standings&league_id=152&APIkey=xxxxxxxxxxxxxx&APIkey=df236cbdeab6cda76f27db6b926edfb358148c92b20e8846d53063f341951d60'
epl_teams_url = 'https://apiv2.apifootball.com/?action=get_teams&league_id=152&APIkey=df236cbdeab6cda76f27db6b926edfb358148c92b20e8846d53063f341951d60'


def extract_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()
        
def extract_league_data(**kwargs):
    data = extract_data(league_url)
    kwargs['ti'].xcom_push(key='league_data', value=data)
        


def league_info(data):?
    results = []
    try:
        for item in data:
            team_standings = {
                'league_id': item['league_id'],
                'league_name': item['league_name'],
                'team_id': item['team_id'],
                'team_name': item['team_name'],
                'overall_league_position': item['overall_league_position'],
                'home_league_GF': item['home_league_GF'],
                'home_league_GA': item['home_league_GA'],
                'away_league_GF': item['away_league_GF'],
                'away_league_GA': item['away_league_GA'],
            }
            results.append(team_standings)
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    ret
    urn results  # Return the list of results

def process_league_data(**kwargs):
    league_data = kwargs['ti'].xcom_pull(task_ids='extract_league_data', key='league_data')
    league_teams = league_info(league_data)
    kwargs['ti'].xcom_push(key='processed_league_data', value=league_teams)


# Extract team info, manager and players
def fetch_clubs_info(data):
    # get teams info before looping through the data to get players info
    teams_info = []
    
    try:
        for team in data:
            for player in team.get('players', []):
                player_info = {
                    'team_key': team['team_key'],
                    'team_name': team['team_name'],
                    "player_key" : player['player_key'],
                    "player_name": player['player_name'],
                    "player_number" :player['player_number'],
                    "player_country": player['player_country'],
                    "player_type": player['player_type'],
                    "player_age": player['player_age'],
                    "player_goals": player['player_goals'],
                    "player_yellow_cards": player['player_yellow_cards'],
                    "player_red_cards": player['player_red_cards'],
                }
                teams_info.append(player_info)
        
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return teams_info  # Return the list of results


def process_team_data(**kwargs):
    team_data = kwargs['ti'].xcom_pull(task_ids='extract_team_data', key='team_data')
    players_info = fetch_clubs_info(team_data)
    kwargs['ti'].xcom_push(key='processed_team_data', value=players_info)

            
league_teams = league_info(league_data)
players_info = fetch_clubs_info(epl_teams_data)
# print(players_info)


def transform_league_data(data):
    transformed_data = []
    try:
        for item in data:
            transformed_item = {
                'league_id': item['league_id'],
                'league_name': item['league_name'],
                'team_id': item['team_id'],
                'team_name': item['team_name'],
                'overall_league_position': item['overall_league_position'],
                'home_league_difference': item['home_league_GF'] - item['home_league_GA'],
                'away_league_difference': item['away_league_GF'] - item['away_league_GA'],
                'goal_difference': (item['home_league_GF'] + item['away_league_GF']) - (item['home_league_GA'] + item['away_league_GA']),
            }
            transformed_data.append(transformed_item)
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return transformed_data


def transform_data(**kwargs):
    processed_league_data = kwargs['ti'].xcom_pull(task_ids='process_league_data', key='processed_league_data')
    transformed_league_data = transform_league_data(processed_league_data)
    kwargs['ti'].xcom_push(key='transformed_league_data', value=transformed_league_data)


league_clubs = transform_league_data(league_teams)
print(league_clubs)

# def load_data(data, output_file):
#     keys = data[0].keys()
#     with open(output_file, 'w', newline='') as f:
#         dict_writer = csv.DictWriter(f, fieldnames=keys)
#         dict_writer.writeheader()
#         dict_writer.writerows(data)
        

import pyodbc

def load_data_to_mssql(league_clubs, players_info, server, database, username, password):
    # Connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    
    # Establish connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Insert league_clubs data
    for club in league_clubs:
        cursor.execute("""
            INSERT INTO league_clubs (league_id, league_name, team_id, team_name, overall_league_position, home_league_difference, away_league_difference, goal_difference)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, club['league_id'], club['league_name'], club['team_id'], club['team_name'], 
            club['overall_league_position'], club['home_league_difference'], 
            club['away_league_difference'], club['goal_difference'])
    
    # Insert players_info data
    for player in players_info:
        cursor.execute("""
            INSERT INTO players_info (
                team_key, team_name, player_key, player_name, 
                player_number, player_country, player_type, player_age, 
                player_goals, player_yellow_cards, player_red_cards
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, player['team_key'], player['team_name'], player['player_key'], 
            player['player_name'], player['player_number'], player['player_country'], 
            player['player_type'], player['player_age'], player['player_goals'], 
            player['player_yellow_cards'], player['player_red_cards'])
    # Commit transaction
    conn.commit()
    
    # Close connection
    cursor.close()
    conn.close()
    
def load_data(**kwargs):
    transformed_league_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_league_data')
    players_info = kwargs['ti'].xcom_pull(task_ids='process_team_data', key='processed_team_data')

    # Replace with your actual database credentials
    server = 'KUFORIJI' 
    database = 'my_db'
    username = 'seyi'
    password = 'seyi'

    load_data_to_mssql(transformed_league_data, players_info, server, database, username, password)

# Define the tasks
extract_league_task = PythonOperator(
    task_id='extract_league_data',
    python_callable=extract_league_data,
    provide_context=True,
    dag=dag,
)

process_league_task = PythonOperator(
    task_id='process_league_data',
    python_callable=process_league_data,
    provide_context=True,
    dag=dag,
)

process_team_task = PythonOperator(
    task_id='process_team_data',
    python_callable=process_team_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_league_task >> process_league_task >> transform_task >> load_task
extract_team_task >> process_team_task >> load_task