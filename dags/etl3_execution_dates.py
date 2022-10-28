import datetime
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator


dag_id = 'etl3_execution_dates'
@dag(
    dag_id=dag_id,
    schedule_interval="15 0 * * *",
    start_date=pendulum.datetime(2018, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    tags=['nba'],
)
def etl3_execution_dates():
    import pandas as pd
    import time

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    
    @task
    def get_games_for_team(team: str) -> str:
        time.sleep(10)       
        teams = pd.read_csv('/opt/airflow/files/teams.csv')
        team_id = teams.loc[teams['ABBREVIATION']==team, 'TEAM_ID']
        games = pd.read_csv('/opt/airflow/files/games.csv')
        home_games = games.loc[games['TEAM_ID_home'].isin(team_id)]
        away_games = games.loc[games['TEAM_ID_away'].isin(team_id)]
        all_games = pd.concat((home_games, away_games))
        out_path = f'/opt/airflow/output/{team}_games.csv'
        all_games.to_csv(out_path, index=False)
        return out_path
    
    @task
    def count_games_within_28_days_of_execution_date(path: str, dt: str) -> int:
        print(f'Exec date is: {dt}')
        games = pd.read_csv(path)
        timedeltas = pd.to_datetime(dt) - games['GAME_DATE_EST'].astype('datetime64[D]')
        idx = (timedeltas < datetime.timedelta(days=28)) & (timedeltas >= datetime.timedelta(days=0))
        games = games.loc[idx]
        print(f'Length of games with 28 days of {dt} is {len(games)}')
        return len(games)

    @task_group(group_id='task_group')
    def count_games_for_team(team: str, dt:str) -> int:
        game_path = get_games_for_team(team)
        game_path << start
        return count_games_within_28_days_of_execution_date(game_path, dt)

    @task
    def print_result(team: str, count: int) -> None:
        print(f'{team} played in {count} games in the dataset')

    teams = ['ATL','BOS','CLE']
    exec_date = '{{ ds }}'
    for team in teams:
        count = count_games_for_team(team, exec_date)
        final = print_result(team, count)
        final >> finish

globals()[dag_id] = etl3_execution_dates()

