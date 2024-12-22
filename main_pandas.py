import sqlite3
import pandas as pd

DATAFILE = 'database.sqlite'

with sqlite3.connect(DATAFILE) as conn:
    pd.set_option("display.max_columns", 10)


    query = "SELECT id, player_api_id FROM Player WHERE player_name like 'Cristiano Ronaldo'"
    player_id = pd.read_sql_query(query, conn)
    player_id = player_id.loc[0, "player_api_id"]


    query = "SELECT id, team_api_id FROM Team WHERE team_long_name like 'FC BARCELONA'"
    team_id = pd.read_sql_query(query, conn)
    team_id = team_id.loc[0, "team_api_id"]


    query = "Select * from Match"
    matches = pd.read_sql_query(query, conn)

    print(player_id)

    players_start_col = matches.columns.get_loc("home_player_1")
    players_end_col = matches.columns.get_loc("away_player_11") + 1

    players_on_pitch = matches.iloc[:, players_start_col:players_end_col]
    cleaned_players = players_on_pitch.dropna()

    print(cleaned_players.isin([player_id]).sum())

    print(f"\n{team_id}")

    teams_start_col = matches.columns.get_loc("home_team_api_id")
    teams_end_col = matches.columns.get_loc("away_team_api_id") + 1

    teams_on_pitch = matches.iloc[:, teams_start_col:teams_end_col]
    cleaned_teams_on_pitch = teams_on_pitch.dropna()

    print(cleaned_teams_on_pitch.isin([team_id]).sum())





