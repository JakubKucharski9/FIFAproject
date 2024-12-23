import sqlite3
from datetime import datetime
from tqdm import tqdm

import pandas as pd
from contourpy.util import data

DATAFILE = 'database.sqlite'

def define_result(data):
    if data['home_team_goal'] > data['away_team_goal']:
        return 1
    elif data['home_team_goal'] < data['away_team_goal']:
        return 2
    elif data['home_team_goal'] == data['away_team_goal']:
        return 0

with sqlite3.connect(DATAFILE) as conn:
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.max_rows", 100)

    query = "Select * from Match_filtered_players"
    matches = pd.read_sql(query, conn)

    unique_matches = matches['id'].unique()
    tqdm.pandas()
    matches['result'] = matches.progress_apply(define_result, axis=1)

    matches.to_sql('Match_filtered_players_result', conn, if_exists='replace', index=False)
