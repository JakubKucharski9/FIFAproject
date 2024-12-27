import sqlite3
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from matplotlib import pyplot as plt

DATAFILE = 'database.sqlite'

dates = {
    '2006/2007': {'start_date': '2006-08-01', 'end_date': '2007-07-31'},
    '2007/2008': {'start_date': '2007-08-01', 'end_date': '2008-07-31'},
    '2008/2009': {'start_date': '2008-08-01', 'end_date': '2009-07-31'},
    '2009/2010': {'start_date': '2009-08-01', 'end_date': '2010-07-31'},
    '2010/2011': {'start_date': '2010-08-01', 'end_date': '2011-07-31'},
    '2011/2012': {'start_date': '2011-08-01', 'end_date': '2012-07-31'},
    '2012/2013': {'start_date': '2012-08-01', 'end_date': '2013-07-31'},
    '2013/2014': {'start_date': '2013-08-01', 'end_date': '2014-07-31'},
    '2014/2015': {'start_date': '2014-08-01', 'end_date': '2015-07-31'},
    '2015/2016': {'start_date': '2015-08-01', 'end_date': '2016-07-31'},
    '2016/2017': {'start_date': '2016-08-01', 'end_date': '2017-07-31'}
}

columns = [
    'id', 'player_fifa_api_id', 'player_api_id', 'date', 'overall_rating',
    'potential', 'preferred_foot', 'attacking_work_rate', 'defensive_work_rate',
    'crossing', 'finishing', 'heading_accuracy', 'short_passing', 'volleys',
    'dribbling', 'curve', 'free_kick_accuracy', 'long_passing', 'ball_control',
    'acceleration', 'sprint_speed', 'agility', 'reactions', 'balance',
    'shot_power', 'jumping', 'stamina', 'strength', 'long_shots', 'aggression',
    'interceptions', 'positioning', 'vision', 'penalties', 'marking',
    'standing_tackle', 'sliding_tackle', 'gk_diving', 'gk_handling',
    'gk_kicking', 'gk_positioning', 'gk_reflexes'
]

def process_fifa_version(data, start_date, end_date):
    filtered = data[(data['date'] >= start_date) & (data['date'] < end_date)]
    if not filtered.empty:
        filtered = filtered.copy()
        filtered['days_diff'] = abs(filtered['date'] - start_date).dt.days
        closest_card = filtered.loc[filtered['days_diff'].idxmin()]
        return {
            'fifa_version': fifa_version,
            **{key: closest_card[key] for key in columns if key in closest_card}
        }

    return None


def define_result(data):
    if data['home_team_goal'] > data['away_team_goal']:
        return 1
    elif data['home_team_goal'] < data['away_team_goal']:
        return 2
    elif data['home_team_goal'] == data['away_team_goal']:
        return 0

data_for_p1_p11 = ['overall_rating', 'potential']

data_for_p2_p11 = ['acceleration', 'sprint_speed', 'stamina', 'jumping', 'strength', 'vision','short_passing', 'crossing',
                   'dribbling', 'curve', 'free_kick_accuracy', 'long_passing', 'ball_control', 'reactions','agility',
                   'balance', 'aggression']

data_for_p1 = ['gk_diving', 'gk_handling', 'gk_kicking', 'gk_positioning', 'gk_reflexes']
data_for_p2_p6 = ['standing_tackle', 'sliding_tackle', 'interceptions', 'heading_accuracy', 'marking']
data_for_p7_p11 = ['positioning', 'long_shots', 'penalties', 'volleys', 'finishing', 'shot_power']

data_for_rename = (data_for_p1_p11 + data_for_p2_p11 + data_for_p1 + data_for_p2_p6 + data_for_p7_p11 +
                   ['defensive_work_rate', 'attacking_work_rate'])

def calculate_avg(data):
    for attribute in columns:
        if attribute in data_for_p1_p11:
            players_home_start_col = data.columns.get_loc(f"home_player_1_{attribute}")
            players_home_end_col = data.columns.get_loc(f"home_player_11_{attribute}") + 1

            players_away_start_col = data.columns.get_loc(f"away_player_1_{attribute}")
            players_away_end_col = data.columns.get_loc(f"away_player_11_{attribute}") + 1

            players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
            players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

            players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
            players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

            data[f'home_avg_{attribute}'] = players_home_rating.mean(axis=1)
            data[f'away_avg_{attribute}'] = players_away_rating.mean(axis=1)

        elif attribute in data_for_p2_p11:
            players_home_start_col = data.columns.get_loc(f"home_player_2_{attribute}")
            players_home_end_col = data.columns.get_loc(f"home_player_11_{attribute}") + 1

            players_away_start_col = data.columns.get_loc(f"away_player_2_{attribute}")
            players_away_end_col = data.columns.get_loc(f"away_player_11_{attribute}") + 1

            players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
            players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

            players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
            players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

            data[f'home_avg_{attribute}'] = players_home_rating.mean(axis=1)
            data[f'away_avg_{attribute}'] = players_away_rating.mean(axis=1)

        elif attribute in data_for_p1:
            players_home_start_col = data.columns.get_loc(f"home_player_1_{attribute}")
            players_home_end_col = data.columns.get_loc(f"home_player_1_{attribute}") + 1

            players_away_start_col = data.columns.get_loc(f"away_player_1_{attribute}")
            players_away_end_col = data.columns.get_loc(f"away_player_1_{attribute}") + 1

            players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
            players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

            players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
            players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

            data[f'home_avg_{attribute}'] = players_home_rating.mean(axis=1)
            data[f'away_avg_{attribute}'] = players_away_rating.mean(axis=1)

        elif attribute in data_for_p2_p6:
            players_home_start_col = data.columns.get_loc(f"home_player_2_{attribute}")
            players_home_end_col = data.columns.get_loc(f"home_player_6_{attribute}") + 1

            players_away_start_col = data.columns.get_loc(f"away_player_2_{attribute}")
            players_away_end_col = data.columns.get_loc(f"away_player_6_{attribute}") + 1

            players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
            players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

            players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
            players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

            data[f'home_avg_{attribute}'] = players_home_rating.mean(axis=1)
            data[f'away_avg_{attribute}'] = players_away_rating.mean(axis=1)

        elif attribute in data_for_p7_p11:
            players_home_start_col = data.columns.get_loc(f"home_player_7_{attribute}")
            players_home_end_col = data.columns.get_loc(f"home_player_11_{attribute}") + 1

            players_away_start_col = data.columns.get_loc(f"away_player_7_{attribute}")
            players_away_end_col = data.columns.get_loc(f"away_player_11_{attribute}") + 1

            players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
            players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

            players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
            players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

            data[f'home_avg_{attribute}'] = players_home_rating.mean(axis=1)
            data[f'away_avg_{attribute}'] = players_away_rating.mean(axis=1)

def convert_work_rate(value):
    mapping = {'low': 1, 'medium': 2, 'high': 3}
    return mapping.get(value, None)


def calculate_work_rate_avg(data, work_rate_type):
    players_home_start_col = data.columns.get_loc(f"home_player_2_{work_rate_type}")
    players_home_end_col = data.columns.get_loc(f"home_player_11_{work_rate_type}") + 1

    players_away_start_col = data.columns.get_loc(f"away_player_2_{work_rate_type}")
    players_away_end_col = data.columns.get_loc(f"away_player_11_{work_rate_type}") + 1

    players_home_work_rate = data.iloc[:, players_home_start_col:players_home_end_col].map(convert_work_rate)
    players_away_work_rate = data.iloc[:, players_away_start_col:players_away_end_col].map(convert_work_rate)

    data[f'home_avg_{work_rate_type}'] = players_home_work_rate.mean(axis=1)
    data[f'away_avg_{work_rate_type}'] = players_away_work_rate.mean(axis=1)


with sqlite3.connect(DATAFILE) as conn:
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.max_rows", 100)

    query = "SELECT * FROM Player_Attributes ORDER BY date"
    player_info = pd.read_sql_query(query, conn)
    player_info = player_info.dropna()
    player_info['date'] = pd.to_datetime(player_info['date'])
    valid_work_rates = {"low", "medium", "high"}
    invalid_rows = player_info[
        (~player_info['attacking_work_rate'].isin(valid_work_rates)) |
        (~player_info['defensive_work_rate'].isin(valid_work_rates))
        ]
    player_info = player_info[
        (player_info['attacking_work_rate'].isin(valid_work_rates)) &
        (player_info['defensive_work_rate'].isin(valid_work_rates))
        ]



    player_info_filtered = []
    for player_id in tqdm(player_info['player_api_id'].unique()):
        player_data = player_info[player_info['player_api_id'] == player_id]
        for fifa_version, date_range in dates.items():
            result = process_fifa_version(
                player_data,
                datetime.strptime(date_range['start_date'], '%Y-%m-%d'),
                datetime.strptime(date_range['end_date'], '%Y-%m-%d')
            )
            if result:
                player_info_filtered.append(result)

    player_info_filtered = pd.DataFrame(player_info_filtered)
    player_info_filtered.to_sql('Player_Attributes_filtered_fifa', conn, if_exists='replace', index=False)

    players_columns_names = ['home_player_', 'away_player_']
    players_columns = [f"{site}{i+1}" for site in players_columns_names for i in range(11)]
    players_query = ", ".join(players_columns)

    query = f"SELECT * FROM Match_avg_bets"
    matches = pd.read_sql_query(query, conn)
    matches = matches.dropna()
    tqdm.pandas()
    matches['result'] = matches.progress_apply(define_result, axis=1)

    for col in tqdm([c for c in matches.columns if c.startswith('home_player_') or c.startswith('away_player_')]):
        temp = matches.merge(
            player_info_filtered,
            left_on=col,
            right_on='player_api_id',
            how='left',
            suffixes=('', '_dup')
        )

        temp = temp[temp['season'] == temp['fifa_version']]
        temp = temp.sort_values(by='date', ascending=False).drop_duplicates(subset=['id', col])
        temp = temp.rename(columns={key: f"{col}_{key}" for key in data_for_rename})

        temp = temp.drop(columns=[c for c in temp.columns if c.endswith('_dup')], errors='ignore')

        matches = matches.merge(
            temp[['id'] + [f"{col}_{key}" for key in data_for_rename if f"{col}_{key}" in temp.columns]],
            on='id',
            how='left'
        )

    matches = matches.dropna()
    calculate_avg(matches)

    calculate_work_rate_avg(matches, "defensive_work_rate")
    calculate_work_rate_avg(matches, "attacking_work_rate")

    print(matches.shape)

    matches.to_sql('Match_with_overall', conn, if_exists='replace', index=False)

