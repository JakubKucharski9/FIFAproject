import sqlite3
from datetime import datetime
from tqdm import tqdm
import pandas as pd

DATAFILE = 'database.sqlite'

# Define the date ranges for FIFA versions, mapping them to corresponding start and end dates
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

# List of player attributes to be used in data processing
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

# Function to filter and process player data for a specific FIFA version
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

# Function to define the result of a match based on goals scored
def define_result(data):
    if data['home_team_goal'] > data['away_team_goal']:
        return 1
    elif data['home_team_goal'] < data['away_team_goal']:
        return 2
    elif data['home_team_goal'] == data['away_team_goal']:
        return 0

# Define attribute groups for players
# Attributes for all the players
data_for_p1_p11 = ['overall_rating', 'potential']

# Attributes for players from field without goalkeeper
data_for_p2_p11 = ['acceleration', 'sprint_speed', 'stamina', 'jumping', 'strength', 'vision','short_passing', 'crossing',
                   'dribbling', 'curve', 'free_kick_accuracy', 'long_passing', 'ball_control', 'reactions','agility',
                   'balance', 'aggression']

# Attributes for the goalkeeper
data_for_p1 = ['gk_diving', 'gk_handling', 'gk_kicking', 'gk_positioning', 'gk_reflexes']
# Attributes for the defenders
data_for_p2_p6 = ['standing_tackle', 'sliding_tackle', 'interceptions', 'heading_accuracy', 'marking']
# Attributes for the attackers
data_for_p7_p11 = ['positioning', 'long_shots', 'penalties', 'volleys', 'finishing', 'shot_power']

# All attributes combined + work rates, used for join function
data_for_rename = (data_for_p1_p11 + data_for_p2_p11 + data_for_p1 + data_for_p2_p6 + data_for_p7_p11 +
                   ['defensive_work_rate', 'attacking_work_rate'])

# Function to calculate average attributes for home and away teams
def calculate_avg(data):
    for attribute in data_for_p1_p11 + data_for_p2_p11 + data_for_p1 + data_for_p2_p6 + data_for_p7_p11:
        home_cols = data.filter(regex=f"home_player_\d+_{attribute}").columns
        away_cols = data.filter(regex=f"away_player_\d+_{attribute}").columns

        data[f'home_avg_{attribute}'] = data[home_cols].mean(axis=1)
        data[f'away_avg_{attribute}'] = data[away_cols].mean(axis=1)

# Function to map work rate values to numerical equivalents
def convert_work_rate(value):
    mapping = {'low': 1, 'medium': 2, 'high': 3}
    return mapping.get(value, None)

# Function to calculate average work rate for home and away teams
def calculate_work_rate_avg(data, work_rate_type):
    players_home_start_col = data.columns.get_loc(f"home_player_2_{work_rate_type}")
    players_home_end_col = data.columns.get_loc(f"home_player_11_{work_rate_type}") + 1

    players_away_start_col = data.columns.get_loc(f"away_player_2_{work_rate_type}")
    players_away_end_col = data.columns.get_loc(f"away_player_11_{work_rate_type}") + 1

    players_home_work_rate = data.iloc[:, players_home_start_col:players_home_end_col].map(convert_work_rate)
    players_away_work_rate = data.iloc[:, players_away_start_col:players_away_end_col].map(convert_work_rate)

    data[f'home_avg_{work_rate_type}'] = players_home_work_rate.mean(axis=1)
    data[f'away_avg_{work_rate_type}'] = players_away_work_rate.mean(axis=1)

# Function to calculate average betting odds for home, draw, and away results
def calculate_avg_bets(df):
    temp = df.iloc[:, df.columns.get_loc("B365H"):]
    df['home_bets_avg'] = temp.filter(regex='H$').mean(axis=1)
    df['draw_bets_avg'] = temp.filter(regex='D$').mean(axis=1)
    df['away_bets_avg'] = temp.filter(regex='A$').mean(axis=1)

# Function to find the predicted outcome based on betting odds
def find_prediction(df):
    cols = ['home_bets_avg', 'draw_bets_avg', 'away_bets_avg']
    df[cols] = df[cols].fillna(float('inf'))
    df['prediction'] = df[cols].idxmin(axis=1).map({
        'home_bets_avg': 1,
        'draw_bets_avg': 0,
        'away_bets_avg': 2
    })

if __name__ == '__main__':
    with sqlite3.connect(DATAFILE) as conn:

        pd.set_option("display.max_columns", 10)
        pd.set_option("display.max_rows", 100)

        query = "SELECT * FROM Player_Attributes ORDER BY date"
        player_info = pd.read_sql_query(query, conn)

        # Drop rows with missing values
        player_info = player_info.dropna()
        player_info['date'] = pd.to_datetime(player_info['date'])

        # Filter out invalid work rate values
        valid_work_rates = {"low", "medium", "high"}
        player_info = player_info[
            (player_info['attacking_work_rate'].isin(valid_work_rates)) &
            (player_info['defensive_work_rate'].isin(valid_work_rates))
            ]

        # Filter player data for each FIFA version
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

        # Convert the filtered player information to a DataFrame
        player_info_filtered = pd.DataFrame(player_info_filtered)
        # Save the filtered player attributes to the database
        player_info_filtered.to_sql('Player_Attributes_filtered_fifa', conn, if_exists='replace', index=False)


        query = "Select * from Match"
        matches = pd.read_sql_query(query, conn)

        # Drop unnecessary columns for faster processing
        matches = matches.drop(columns=matches.loc[:, 'home_player_X1':'away_player_Y11'], axis=1)
        matches = matches.drop(columns=matches.loc[:, 'goal':'possession'], axis=1)

        # Calculate average betting odds
        calculate_avg_bets(matches)
        # Add predictions to the match data
        find_prediction(matches)

        # Add progress bar
        tqdm.pandas()
        # Calculate match results using the `define_result` function
        matches['result'] = matches.progress_apply(define_result, axis=1)

        # Merge player attributes with match data
        for col in tqdm([c for c in matches.columns if c.startswith('home_player_') or c.startswith('away_player_')]):
            temp = matches.merge(
                player_info_filtered,
                left_on=col,
                right_on='player_api_id',
                how='left',
                suffixes=('', '_dup')
            )

            # Filter for relevant FIFA version and drop duplicates
            temp = temp[temp['season'] == temp['fifa_version']]
            temp = temp.sort_values(by='date', ascending=False).drop_duplicates(subset=['id', col])
            temp = temp.rename(columns={key: f"{col}_{key}" for key in data_for_rename})

            # Drop duplicate columns
            temp = temp.drop(columns=[c for c in temp.columns if c.endswith('_dup')], errors='ignore')

            # Merge the processed data back to matches
            matches = matches.merge(
                temp[['id'] + [f"{col}_{key}" for key in data_for_rename if f"{col}_{key}" in temp.columns]],
                on='id',
                how='left'
            )

        # Remove rows with missing data after merging
        matches = matches.dropna()
        # Calculate averages for attributes
        calculate_avg(matches)
        # Calculate average work rates for home and away teams
        calculate_work_rate_avg(matches, "defensive_work_rate")
        calculate_work_rate_avg(matches, "attacking_work_rate")
        # Save the processed match data to the database
        matches.to_sql('Match_for_plots', conn, if_exists='replace', index=False)

