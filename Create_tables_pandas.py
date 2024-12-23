import sqlite3
from datetime import datetime
from tqdm import tqdm

import pandas as pd
from contourpy.util import data

DATAFILE = 'database.sqlite'

dates = {
    'FIFA 07': {'start_date': '2006-08-01', 'end_date': '2007-07-31'},
    'FIFA 08': {'start_date': '2007-08-01', 'end_date': '2008-07-31'},
    'FIFA 09': {'start_date': '2008-08-01', 'end_date': '2009-07-31'},
    'FIFA 10': {'start_date': '2009-08-01', 'end_date': '2010-07-31'},
    'FIFA 11': {'start_date': '2010-08-01', 'end_date': '2011-07-31'},
    'FIFA 12': {'start_date': '2011-08-01', 'end_date': '2012-07-31'},
    'FIFA 13': {'start_date': '2012-08-01', 'end_date': '2013-07-31'},
    'FIFA 14': {'start_date': '2013-08-01', 'end_date': '2014-07-31'},
    'FIFA 15': {'start_date': '2014-08-01', 'end_date': '2015-07-31'},
    'FIFA 16': {'start_date': '2015-08-01', 'end_date': '2016-07-31'},
    'FIFA 17': {'start_date': '2016-08-01', 'end_date': '2017-07-31'}
}

def process_fifa_version(data, start_date, end_date, fifa_version):
    filtered = data[
        (data['date'] >= start_date) &
        (data['date'] < end_date)
    ]
    if not filtered.empty:
        filtered = filtered.copy()
        filtered['days_diff'] = abs(filtered['date'] - start_date).dt.days
        closest_card = filtered.loc[filtered['days_diff'].idxmin()]
        return {
            'fifa_version': fifa_version,
            'id': closest_card['id'],
            'player_api_id': closest_card['player_api_id'],
            'overall_rating': closest_card['overall_rating'],
            'date': closest_card['date']
        }
    return None

with sqlite3.connect(DATAFILE) as conn:
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.max_rows", 100)


    query = "Select id, player_api_id, overall_rating, date from Player_Attributes order by date "
    player_info = pd.read_sql_query(query, conn)
    player_info = player_info.dropna()
    player_info['date'] = pd.to_datetime(player_info['date'])


    date_ranges = pd.DataFrame([
        {'fifa_version': key, 'start_date': datetime.strptime(value['start_date'], '%Y-%m-%d'),
         'end_date': datetime.strptime(value['end_date'], '%Y-%m-%d')}
        for key, value in dates.items()
    ])

    unique_players = player_info['id'].unique()


    player_info_filtered = []

    for player_id in tqdm(unique_players):
        player_data = player_info[player_info['player_api_id'] == player_id]
        for fifa_version, date_range in dates.items():
            result = process_fifa_version(
                player_data,
                datetime.strptime(date_range['start_date'], '%Y-%m-%d'),
                datetime.strptime(date_range['end_date'], '%Y-%m-%d'),
                fifa_version
            )
            if result:
                player_info_filtered.append(result)

    player_info_filtered = pd.DataFrame(player_info_filtered)
    player_info_filtered.to_sql('Player_Attributes_filtered_fifa', conn, if_exists='replace', index=False)
    print(player_info_filtered.head(100))


    players_columns_names = ['home_player_', 'away_player_']
    players_columns = []
    for site in players_columns_names:
        for i in range(11):
            players_columns.append(site + str(i+1))

    players_query = ", ".join(players_columns)

    query = f"Select id, season, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal, {players_query} from Match"
    matches = pd.read_sql_query(query, conn)

    matches = matches.dropna()
    matches.to_sql('Match_filtered_players', conn, if_exists='replace', index=False)

    print(matches.columns)
