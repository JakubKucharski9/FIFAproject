import sqlite3

conn = sqlite3.connect('database.sqlite')
cursor = conn.cursor()

cursor.execute("""
                    Select id, home_team_goal, away_team_goal, whh, whd, wha from Match
                    where home_player_X1 is not null and
                    whh is not null and
                    whd is not null and
                    wha is not null and
                    (whh > wha and home_team_goal > away_team_goal) or
                    (whh < wha and home_team_goal < away_team_goal)
                """)

query = cursor.fetchall()


print(len(query))



conn.close()