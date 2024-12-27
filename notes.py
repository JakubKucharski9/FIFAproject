import math
import sqlite3

import pandas as pd
from tqdm import tqdm


def calculate_avg_bets(df):
    temp = df.iloc[:, df.columns.get_loc("B365H"):]
    df['home_bets_avg'] = temp.filter(regex='H$').mean(axis=1)
    df['draw_bets_avg'] = temp.filter(regex='D$').mean(axis=1)
    df['away_bets_avg'] = temp.filter(regex='A$').mean(axis=1)

def find_prediction(df):
    # Znajdź minimalną kolumnę dla każdej wartości
    cols = ['home_bets_avg', 'draw_bets_avg', 'away_bets_avg']
    df[cols] = df[cols].fillna(float('inf'))
    df['prediction'] = df[cols].idxmin(axis=1).map({
        'home_bets_avg': 1,
        'draw_bets_avg': 0,
        'away_bets_avg': 2
    })

with sqlite3.connect('database.sqlite') as conn:
    q = "Select * from Match"
    df = pd.read_sql_query(q, conn)
    df = df.drop(columns=df.loc[:,'home_player_X1':'away_player_Y11'], axis=1)
    df = df.drop(columns=df.loc[:, 'goal':'possession'], axis=1)

    calculate_avg_bets(df)
    find_prediction(df)
    df.to_sql('Match_avg_bets', conn, if_exists='replace', index=False)
