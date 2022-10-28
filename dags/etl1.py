import datetime
import pendulum
from airflow.decorators import dag, task, task_group

dag_id = 'etl1'
@dag(
    dag_id=dag_id,
    schedule_interval="15 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    tags=['nba'],
)
def etl1():
    import pandas as pd
    from airflow.operators.empty import EmptyOperator
    import time

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
        raise ValueError('Bad values!')
        return out_path
    
    @task
    def count_games(path: str) -> int:
        games = pd.read_csv(path)
        print(f'Length of games is {len(games)}')
        return len(games)
    
    @task
    def print_result(team: str, count: int) -> None:
        print(f'{team} played in {count} games in the dataset')

    team = 'ATL'
    game_path = get_games_for_team(team)
    final = print_result(team, count_games(game_path))

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    start >> game_path
    finish << final

globals()[dag_id] = etl1()

