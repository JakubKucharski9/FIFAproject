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


def process_fifa_version(data, start_date, end_date, fifa_version):
    filtered = data[(data['date'] >= start_date) & (data['date'] < end_date)]
    if not filtered.empty:
        filtered = filtered.copy()
        filtered['days_diff'] = abs(filtered['date'] - start_date).dt.days
        closest_card = filtered.loc[filtered['days_diff'].idxmin()]
        return {
            'fifa_version': fifa_version,
            'id': closest_card['id'],
            'player_api_id': closest_card['player_api_id'],
            'overall_rating': closest_card['overall_rating'],
            'date': closest_card['date'],
            'defensive_work_rate': closest_card['defensive_work_rate'],
            'attacking_work_rate': closest_card['attacking_work_rate']
        }
    return None


def define_result(data):
    if data['home_team_goal'] > data['away_team_goal']:
        return 1
    elif data['home_team_goal'] < data['away_team_goal']:
        return 2
    return 0


def calculate_avg(data):
    players_home_start_col = data.columns.get_loc("home_player_1_rating")
    players_home_end_col = data.columns.get_loc("home_player_11_rating") + 1

    players_away_start_col = data.columns.get_loc("away_player_1_rating")
    players_away_end_col = data.columns.get_loc("away_player_11_rating") + 1

    # Pobranie kolumn dla drużyn
    players_home_rating = data.iloc[:, players_home_start_col:players_home_end_col]
    players_away_rating = data.iloc[:, players_away_start_col:players_away_end_col]

    # Konwersja do numerycznego, ignorowanie błędów
    players_home_rating = players_home_rating.apply(pd.to_numeric, errors='coerce')
    players_away_rating = players_away_rating.apply(pd.to_numeric, errors='coerce')

    # Obliczenie średnich
    data['home_avg_overall'] = players_home_rating.mean(axis=1)
    data['away_avg_overall'] = players_away_rating.mean(axis=1)


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
                datetime.strptime(date_range['end_date'], '%Y-%m-%d'),
                fifa_version
            )
            if result:
                player_info_filtered.append(result)

    player_info_filtered = pd.DataFrame(player_info_filtered)
    player_info_filtered.to_sql('Player_Attributes_filtered_fifa', conn, if_exists='replace', index=False)

    players_columns_names = ['home_player_', 'away_player_']
    players_columns = [f"{site}{i+1}" for site in players_columns_names for i in range(11)]
    players_query = ", ".join(players_columns)

    query = f"SELECT id, season, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal, {players_query} FROM Match"
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
        temp = temp.rename(columns={
            'overall_rating': f'{col}_rating',
            'attacking_work_rate': f'{col}_attacking_work_rate',
            'defensive_work_rate': f'{col}_defensive_work_rate'
        })

        temp = temp.drop(columns=[c for c in temp.columns if c.endswith('_dup')], errors='ignore')

        matches = matches.merge(
            temp[['id', f'{col}_rating', f'{col}_attacking_work_rate', f'{col}_defensive_work_rate']],
            on='id',
            how='left'
        )

    matches = matches.dropna()
    calculate_avg(matches)

    calculate_work_rate_avg(matches, "defensive_work_rate")
    calculate_work_rate_avg(matches, "attacking_work_rate")

    matches.to_sql('Match_with_overall', conn, if_exists='replace', index=False)

    color_map = {0: 'gray', 1: 'blue', 2: 'red'}
    colors = matches['result'].map(color_map)

    plt.figure(figsize=(10, 6))
    plt.scatter(matches['home_avg_overall'], matches['away_avg_overall'], c=colors, alpha=0.7, edgecolors='k')
    min_value = min(matches['home_avg_overall'].min(), matches['away_avg_overall'].min())
    max_value = max(matches['home_avg_overall'].max(), matches['away_avg_overall'].max())
    plt.plot([min_value, max_value], [min_value, max_value], color='green', linestyle='--')

    plt.xlabel('Home Team Average Overall', fontsize=12)
    plt.ylabel('Away Team Average Overall', fontsize=12)
    plt.title('Match Results by Team Average Overall Ratings', fontsize=14)

    for result, color in color_map.items():
        plt.scatter([], [], c=color, label=f'Result: {result} ({"Home Win" if result == 1 else "Away Win" if result == 2 else "Draw"})')
    plt.legend(title='Match Result')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.show()



    plt.figure(figsize=(12, 6))
    plt.scatter(
        matches['home_avg_defensive_work_rate'],
        matches['away_team_goal'],
        c='blue', alpha=0.7, label='Home Teams'
    )
    plt.scatter(
        matches['away_avg_defensive_work_rate'],
        matches['home_team_goal'],
        c='red', alpha=0.7, label='Away Teams'
    )
    plt.xlabel('Average Defensive Work Rate', fontsize=12)
    plt.ylabel('Goals Conceded', fontsize=12)
    plt.title('Defensive Work Rate vs Goals Conceded', fontsize=14)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.scatter(
        matches['home_avg_attacking_work_rate'],
        matches['home_team_goal'],
        c='blue', alpha=0.7, label='Home Teams'
    )
    plt.scatter(
        matches['away_avg_attacking_work_rate'],
        matches['away_team_goal'],
        c='red', alpha=0.7, label='Away Teams'
    )
    plt.xlabel('Average Attacking Work Rate', fontsize=12)
    plt.ylabel('Goals Scored', fontsize=12)
    plt.title('Attacking Work Rate vs Goals Scored', fontsize=14)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.show()