import sqlite3

with sqlite3.connect('database.sqlite') as conn:
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info('Match')")
    query = cursor.fetchall()
    column_names = [column[1] for column in query]
    books = column_names[85:]

    shift = "\n\t\t\t\t\t"

    conditions_books = f" AND {shift}".join([f"{book} IS NOT NULL" for book in books])
    conditions_goals = f"{shift}home_team_goal > away_team_goal OR {shift}home_team_goal < away_team_goal"
    full_conditions = f"{conditions_books} AND {conditions_goals}"

    sql_ask = f"""
                    Select id, home_team_goal, away_team_goal, whh, whd, wha
                    from Match
                    where {full_conditions}
                    """
    cursor.execute(sql_ask)
    query = cursor.fetchall()
    print(len(query))